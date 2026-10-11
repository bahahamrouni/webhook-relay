import importlib.util
import tempfile
import unittest
from pathlib import Path

from webhook_relay.config import Config
from webhook_relay.store import Store

HAS_FASTAPI = (
    importlib.util.find_spec("fastapi") is not None
    and importlib.util.find_spec("httpx") is not None
)
KEY = "test-api-key-0123456789"
AUTH = {"X-API-Key": KEY}


@unittest.skipUnless(HAS_FASTAPI, "fastapi and httpx are required (pip install -e .[dev])")
class ApiTests(unittest.TestCase):
    def setUp(self):
        from fastapi.testclient import TestClient

        from webhook_relay.api import create_app

        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.store = Store(str(Path(self._tmp.name) / "relay.db"))
        self.cfg = Config(db_path="unused", api_key=KEY, allow_private_targets=True)
        self.client = TestClient(create_app(self.store, self.cfg))

    def subscribe(self, **extra):
        body = {"url": "http://127.0.0.1:9000/hook", **extra}
        response = self.client.post("/v1/subscriptions", json=body, headers=AUTH)
        self.assertEqual(response.status_code, 201, response.text)
        return response.json()

    def test_health_is_public_and_everything_else_needs_the_key(self):
        self.assertEqual(self.client.get("/healthz").json(), {"status": "ok"})
        for method, path in [
            ("get", "/v1/subscriptions"), ("get", "/v1/deliveries"),
            ("get", "/v1/stats"), ("get", "/metrics"),
        ]:
            self.assertEqual(getattr(self.client, method)(path).status_code, 401)
            wrong = getattr(self.client, method)(path, headers={"X-API-Key": "wrong"})
            self.assertEqual(wrong.status_code, 401)
        unauthenticated = self.client.post("/v1/events", json={"type": "a", "data": {}})
        self.assertEqual(unauthenticated.status_code, 401)

    def test_app_refuses_to_start_without_a_strong_api_key(self):
        from webhook_relay.api import create_app

        for key in (None, "", "short"):
            with self.subTest(key=key), self.assertRaises(ValueError):
                create_app(self.store, Config(db_path="unused", api_key=key))

    def test_subscription_secret_is_returned_once(self):
        created = self.subscribe(event_types=["order.*"])
        self.assertTrue(len(created["secret"]) >= 32)
        listed = self.client.get("/v1/subscriptions", headers=AUTH).json()
        self.assertEqual([s["id"] for s in listed], [created["id"]])
        self.assertNotIn("secret", listed[0])

    def test_private_targets_are_rejected_unless_allowed(self):
        from fastapi.testclient import TestClient

        from webhook_relay.api import create_app

        strict = TestClient(create_app(self.store, Config(db_path="unused", api_key=KEY)))
        response = strict.post(
            "/v1/subscriptions", json={"url": "http://169.254.169.254/x"}, headers=AUTH
        )
        self.assertEqual(response.status_code, 422)

    def test_event_creates_deliveries_and_is_idempotent(self):
        self.subscribe()
        headers = {**AUTH, "Idempotency-Key": "abc-123"}
        event = {"type": "order.created", "data": {"n": 1}}
        first = self.client.post("/v1/events", json=event, headers=headers)
        again = self.client.post("/v1/events", json=event, headers=headers)
        self.assertEqual((first.status_code, again.status_code), (202, 200))
        self.assertEqual(first.json()["id"], again.json()["id"])
        self.assertEqual(first.json()["deliveries"], 1)
        listed = self.client.get("/v1/deliveries", headers=AUTH).json()
        self.assertEqual(len(listed), 1)
        self.assertEqual(self.client.get("/v1/stats", headers=AUTH).json()["pending"], 1)
        metrics = self.client.get("/metrics", headers=AUTH).text
        self.assertIn('relay_deliveries{status="pending"} 1', metrics)

    def test_event_validation(self):
        for body in [{"type": "bad type!", "data": {}}, {"type": "", "data": {}}, {"type": "a.b"}]:
            with self.subTest(body=body):
                self.assertEqual(self.client.post("/v1/events", json=body, headers=AUTH).status_code, 422)

    def test_oversized_event_is_rejected(self):
        from fastapi.testclient import TestClient

        from webhook_relay.api import create_app

        small = Config(db_path="unused", api_key=KEY, max_body_bytes=50)
        client = TestClient(create_app(self.store, small))
        big = {"type": "a.b", "data": {"x": "y" * 100}}
        response = client.post("/v1/events", json=big, headers=AUTH)
        self.assertEqual(response.status_code, 413)

    def test_retry_endpoint_only_accepts_dead_deliveries(self):
        self.subscribe()
        self.client.post("/v1/events", json={"type": "a.b", "data": {}}, headers=AUTH)
        delivery_id = self.client.get("/v1/deliveries", headers=AUTH).json()[0]["id"]
        retry_url = f"/v1/deliveries/{delivery_id}/retry"
        self.assertEqual(self.client.post(retry_url, headers=AUTH).status_code, 409)
        self.assertEqual(self.client.post("/v1/deliveries/dlv_nope/retry", headers=AUTH).status_code, 404)
        self.store.mark_dead(delivery_id, 1.0, 400, "HTTP 400")
        self.assertEqual(self.client.post(retry_url, headers=AUTH).json(), {"status": "pending"})
        fetched = self.client.get(f"/v1/deliveries/{delivery_id}", headers=AUTH).json()
        self.assertEqual(fetched["status"], "pending")

    def test_delete_subscription_and_status_filter(self):
        sub = self.subscribe()
        url = f"/v1/subscriptions/{sub['id']}"
        self.assertEqual(self.client.delete(url, headers=AUTH).status_code, 204)
        self.assertEqual(self.client.delete(url, headers=AUTH).status_code, 404)
        self.assertEqual(self.client.get("/v1/deliveries?status=bogus", headers=AUTH).status_code, 422)
        self.assertEqual(self.client.get("/v1/deliveries?status=dead", headers=AUTH).json(), [])


if __name__ == "__main__":
    unittest.main()
