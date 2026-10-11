import tempfile
import threading
import unittest
from pathlib import Path

from webhook_relay.store import Store, event_type_matches

T0 = 1_000_000.0


class StoreTestCase(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.store = Store(str(Path(self._tmp.name) / "relay.db"))

    def subscribe(self, event_types=("*",), url="http://example.test/hook"):
        return self.store.create_subscription(url, list(event_types), "secret-" * 3, T0)


class MatchingTests(unittest.TestCase):
    def test_patterns(self):
        self.assertTrue(event_type_matches("*", "anything.at.all"))
        self.assertTrue(event_type_matches("order.*", "order.created"))
        self.assertFalse(event_type_matches("order.*", "orders.created"))
        self.assertFalse(event_type_matches("order.*", "order"))
        self.assertTrue(event_type_matches("user.created", "user.created"))
        self.assertFalse(event_type_matches("user.created", "user.deleted"))


class IngestTests(StoreTestCase):
    def test_fan_out_only_to_matching_active_subscriptions(self):
        self.subscribe(["order.*"])
        self.subscribe(["*"])
        self.subscribe(["user.created"])
        inactive = self.subscribe(["*"])
        self.store.deactivate_subscription(inactive.id)
        result = self.store.ingest_event("order.created", {"n": 1}, None, T0)
        self.assertTrue(result.created)
        self.assertEqual(result.deliveries, 2)
        self.assertEqual(self.store.stats()["pending"], 2)

    def test_event_with_no_subscribers_is_accepted(self):
        result = self.store.ingest_event("order.created", {}, None, T0)
        self.assertEqual((result.created, result.deliveries), (True, 0))

    def test_idempotency_key_prevents_duplicates(self):
        self.subscribe()
        first = self.store.ingest_event("a.b", {"x": 1}, "key-1", T0)
        second = self.store.ingest_event("a.b", {"x": 1}, "key-1", T0 + 5)
        self.assertTrue(first.created)
        self.assertFalse(second.created)
        self.assertEqual(first.event_id, second.event_id)
        self.assertEqual(second.deliveries, 1)
        self.assertEqual(self.store.stats()["pending"], 1)


class QueueTests(StoreTestCase):
    def test_claim_is_exclusive(self):
        self.subscribe()
        self.store.ingest_event("a.b", {}, None, T0)
        first = self.store.claim_due(T0, 10, 30)
        second = self.store.claim_due(T0, 10, 30)
        self.assertEqual(len(first), 1)
        self.assertEqual(second, [])
        self.assertEqual(first[0].attempts, 1)
        self.assertEqual(self.store.stats()["in_flight"], 1)

    def test_expired_lease_is_reclaimed_and_counts_an_attempt(self):
        self.subscribe()
        self.store.ingest_event("a.b", {}, None, T0)
        self.store.claim_due(T0, 10, 30)
        self.assertEqual(self.store.claim_due(T0 + 29, 10, 30), [])
        again = self.store.claim_due(T0 + 31, 10, 30)
        self.assertEqual(len(again), 1)
        self.assertEqual(again[0].attempts, 2)

    def test_retry_is_not_due_until_its_time(self):
        self.subscribe()
        self.store.ingest_event("a.b", {}, None, T0)
        job = self.store.claim_due(T0, 10, 30)[0]
        self.store.mark_retry(job.id, T0, T0 + 30, 503, "HTTP 503")
        self.assertEqual(self.store.claim_due(T0 + 29, 10, 30), [])
        self.assertEqual(len(self.store.claim_due(T0 + 30, 10, 30)), 1)

    def test_deactivated_subscription_deliveries_become_dead(self):
        sub = self.subscribe()
        self.store.ingest_event("a.b", {}, None, T0)
        self.store.deactivate_subscription(sub.id)
        self.assertEqual(self.store.claim_due(T0, 10, 30), [])
        self.assertEqual(self.store.stats()["dead"], 1)

    def test_requeue_only_applies_to_dead_deliveries(self):
        self.subscribe()
        self.store.ingest_event("a.b", {}, None, T0)
        job = self.store.claim_due(T0, 10, 30)[0]
        self.assertEqual(self.store.requeue_dead(job.id, T0), "not_dead")
        self.assertEqual(self.store.requeue_dead("dlv_missing", T0), "not_found")
        self.store.mark_dead(job.id, T0, 400, "HTTP 400")
        self.assertEqual(self.store.requeue_dead(job.id, T0 + 1), "requeued")
        delivery = self.store.get_delivery(job.id)
        self.assertEqual((delivery["status"], delivery["attempts"]), ("pending", 0))

    def test_concurrent_claims_never_hand_out_a_job_twice(self):
        self.subscribe()
        for i in range(40):
            self.store.ingest_event("a.b", {"i": i}, None, T0)
        claimed: list[str] = []
        lock = threading.Lock()

        def worker():
            while True:
                jobs = self.store.claim_due(T0, 5, 30)
                if not jobs:
                    return
                with lock:
                    claimed.extend(j.id for j in jobs)

        threads = [threading.Thread(target=worker) for _ in range(6)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(len(claimed), 40)
        self.assertEqual(len(set(claimed)), 40)

    def test_listing_and_filters(self):
        self.subscribe()
        a = self.store.ingest_event("a.b", {}, None, T0)
        self.store.ingest_event("a.b", {}, None, T0 + 1)
        self.assertEqual(len(self.store.list_deliveries()), 2)
        self.assertEqual(len(self.store.list_deliveries(event_id=a.event_id)), 1)
        self.assertEqual(len(self.store.list_deliveries(status="dead")), 0)
        self.assertEqual(len(self.store.list_deliveries(limit=1)), 1)


if __name__ == "__main__":
    unittest.main()
