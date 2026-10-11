import json
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from webhook_relay.config import Config
from webhook_relay.signing import verify_signature
from webhook_relay.store import Store
from webhook_relay.worker import SendResult, UrllibSender, classify, process_batch

T0 = 1_000_000.0
SECRET = "worker-test-secret-0123456789"


class _MaxRng:
    """Always picks the top of the jitter range so retry times are predictable."""

    def uniform(self, a, b):
        return b


class FakeSender:
    def __init__(self, *results):
        self.results = list(results)
        self.calls = []

    def send(self, url, headers, body, timeout):
        self.calls.append((url, headers, body, timeout))
        return self.results.pop(0) if self.results else SendResult(200)


class WorkerTestCase(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.store = Store(str(Path(self._tmp.name) / "relay.db"))
        self.cfg = Config(
            db_path="unused", max_attempts=3, base_delay=2, max_delay=60,
            lease_seconds=30, delivery_timeout=5, concurrency=1,
        )

    def publish(self, url="http://example.test/hook", event_type="order.created"):
        self.store.create_subscription(url, ["*"], SECRET, T0)
        return self.store.ingest_event(event_type, {"order": 7}, None, T0)

    def run_batch(self, sender, now):
        return process_batch(self.store, sender, self.cfg, now=now, rng=_MaxRng())

    def only_delivery(self):
        rows = self.store.list_deliveries()
        self.assertEqual(len(rows), 1)
        return rows[0]


class ClassifyTests(unittest.TestCase):
    def test_classification_table(self):
        cases = {
            200: "delivered", 204: "delivered", 299: "delivered",
            301: "dead", 302: "dead", 400: "dead", 401: "dead", 404: "dead", 410: "dead",
            408: "retry", 425: "retry", 429: "retry", 500: "retry", 503: "retry",
        }
        for code, expected in cases.items():
            with self.subTest(code=code):
                self.assertEqual(classify(SendResult(code)), expected)
        self.assertEqual(classify(SendResult(None, "timeout")), "retry")
        self.assertEqual(classify(SendResult(None, "unsafe", permanent=True)), "dead")


class ProcessBatchTests(WorkerTestCase):
    def test_success_on_first_attempt_sends_a_valid_signed_request(self):
        result = self.publish()
        sender = FakeSender(SendResult(200))
        self.assertEqual(self.run_batch(sender, T0), 1)
        delivery = self.only_delivery()
        self.assertEqual((delivery["status"], delivery["attempts"]), ("delivered", 1))
        url, headers, body, timeout = sender.calls[0]
        self.assertEqual(url, "http://example.test/hook")
        self.assertEqual(timeout, 5)
        self.assertEqual(headers["X-Relay-Event-Id"], result.event_id)
        self.assertEqual(headers["X-Relay-Attempt"], "1")
        verify_signature(SECRET, headers["X-Relay-Signature"], body, now=T0)
        envelope = json.loads(body)
        self.assertEqual(envelope["id"], result.event_id)
        self.assertEqual(envelope["type"], "order.created")
        self.assertEqual(envelope["data"], {"order": 7})

    def test_server_error_is_retried_with_backoff_then_delivered(self):
        self.publish()
        sender = FakeSender(SendResult(500, "HTTP 500"), SendResult(200))
        self.run_batch(sender, T0)
        delivery = self.only_delivery()
        self.assertEqual(delivery["status"], "pending")
        self.assertEqual(delivery["next_attempt_at"], T0 + 2)  # attempt 1 ceiling = base
        self.assertEqual(delivery["last_status_code"], 500)
        self.assertEqual(self.run_batch(sender, T0 + 1), 0)  # not due yet
        self.assertEqual(self.run_batch(sender, T0 + 2), 1)
        delivery = self.only_delivery()
        self.assertEqual((delivery["status"], delivery["attempts"]), ("delivered", 2))

    def test_permanent_client_error_goes_straight_to_dead(self):
        self.publish()
        self.run_batch(FakeSender(SendResult(400, "HTTP 400")), T0)
        delivery = self.only_delivery()
        self.assertEqual((delivery["status"], delivery["attempts"]), ("dead", 1))
        self.assertEqual(delivery["last_status_code"], 400)

    def test_retries_are_exhausted_after_max_attempts(self):
        self.publish()
        sender = FakeSender(*[SendResult(503, "HTTP 503")] * 5)
        times = [T0, T0 + 2, T0 + 2 + 4]  # ceilings 2s then 4s, jitter pinned to max
        for now in times:
            self.assertEqual(self.run_batch(sender, now), 1)
        delivery = self.only_delivery()
        self.assertEqual((delivery["status"], delivery["attempts"]), ("dead", 3))
        self.assertIn("attempts exhausted", delivery["last_error"])
        self.assertEqual(self.run_batch(sender, T0 + 1000), 0)
        self.assertEqual(len(sender.calls), 3)

    def test_network_error_is_retried(self):
        self.publish()
        self.run_batch(FakeSender(SendResult(None, "URLError: refused")), T0)
        delivery = self.only_delivery()
        self.assertEqual(delivery["status"], "pending")
        self.assertIsNone(delivery["last_status_code"])

    def test_unsafe_target_is_dead_without_retry(self):
        self.publish()
        self.run_batch(FakeSender(SendResult(None, "unsafe target", permanent=True)), T0)
        self.assertEqual(self.only_delivery()["status"], "dead")

    def test_dead_delivery_can_be_requeued_and_then_delivered(self):
        self.publish()
        self.run_batch(FakeSender(SendResult(400)), T0)
        delivery_id = self.only_delivery()["id"]
        self.assertEqual(self.store.requeue_dead(delivery_id, T0 + 10), "requeued")
        self.run_batch(FakeSender(SendResult(200)), T0 + 10)
        self.assertEqual(self.only_delivery()["status"], "delivered")

    def test_jobs_whose_leases_keep_expiring_are_eventually_dead(self):
        self.publish()
        for i in range(self.cfg.max_attempts):  # claimed but never processed (worker crash)
            self.store.claim_due(T0 + i * 40, 10, self.cfg.lease_seconds)
        self.assertEqual(self.run_batch(FakeSender(), T0 + 500), 1)
        delivery = self.only_delivery()
        self.assertEqual(delivery["status"], "dead")
        self.assertIn("attempt limit", delivery["last_error"])

    def test_one_event_fans_out_to_every_subscriber(self):
        self.store.create_subscription("http://a.test/h", ["*"], SECRET, T0)
        self.store.create_subscription("http://b.test/h", ["order.*"], SECRET, T0)
        self.store.ingest_event("order.created", {}, None, T0)
        sender = FakeSender()
        self.assertEqual(self.run_batch(sender, T0), 2)
        self.assertEqual({c[0] for c in sender.calls}, {"http://a.test/h", "http://b.test/h"})


class _Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length)
        self.server.received.append(({k.lower(): v for k, v in self.headers.items()}, body))
        status = self.server.script.pop(0) if self.server.script else 200
        self.send_response(status)
        if status in (301, 302):
            self.send_header("Location", "http://127.0.0.1:9/elsewhere")
        self.send_header("Content-Length", "0")
        self.end_headers()

    def log_message(self, *args):
        pass


class EndToEndTests(WorkerTestCase):
    """The real HTTP sender against a real local server."""

    def start_server(self, script):
        server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
        server.received, server.script = [], list(script)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(thread.join, 5)
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        return server, f"http://127.0.0.1:{server.server_address[1]}/hook"

    def test_retry_then_success_over_http_with_valid_signatures(self):
        server, url = self.start_server([500, 200])
        self.publish(url=url)
        sender = UrllibSender(allow_private=True)
        self.run_batch(sender, T0)
        self.run_batch(sender, T0 + 2)
        self.assertEqual(self.only_delivery()["status"], "delivered")
        self.assertEqual(len(server.received), 2)
        for headers, body in server.received:
            verify_signature(SECRET, headers["x-relay-signature"], body, now=headers_ts(headers))
        self.assertEqual([h["x-relay-attempt"] for h, _ in server.received], ["1", "2"])

    def test_redirects_are_not_followed(self):
        server, url = self.start_server([302])
        self.publish(url=url)
        self.run_batch(UrllibSender(allow_private=True), T0)
        delivery = self.only_delivery()
        self.assertEqual((delivery["status"], delivery["last_status_code"]), ("dead", 302))
        self.assertEqual(len(server.received), 1)

    def test_loopback_target_is_refused_by_default(self):
        server, url = self.start_server([200])
        self.publish(url=url)
        self.run_batch(UrllibSender(), T0)
        self.assertEqual(self.only_delivery()["status"], "dead")
        self.assertIn("unsafe target", self.only_delivery()["last_error"])
        self.assertEqual(server.received, [])

    def test_connection_refused_is_retried(self):
        self.publish(url="http://127.0.0.1:9/hook")  # discard port: nothing listening
        self.run_batch(UrllibSender(allow_private=True), T0)
        self.assertEqual(self.only_delivery()["status"], "pending")


def headers_ts(headers):
    """The signed timestamp, so verification can run at that exact moment."""
    return int(headers["x-relay-signature"].split(",")[0].split("=")[1])


if __name__ == "__main__":
    unittest.main()
