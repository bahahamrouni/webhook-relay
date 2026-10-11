"""Tiny webhook receiver for demos. It verifies the signature before trusting a request.

    RELAY_SECRET=<secret returned when you created the subscription> python examples/receiver.py

Then subscribe  http://127.0.0.1:9000/hook  (set RELAY_ALLOW_PRIVATE_TARGETS=1 on the relay).
Receivers must be idempotent: delivery is at-least-once, so use the event id to deduplicate.
"""

import json
import os
from http.server import BaseHTTPRequestHandler, HTTPServer

from webhook_relay.signing import SignatureError, verify_signature

SECRET = os.environ["RELAY_SECRET"]
seen: set[str] = set()


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
        try:
            verify_signature(SECRET, self.headers.get("X-Relay-Signature", ""), body)
        except SignatureError as exc:
            self.send_response(401)
            self.end_headers()
            print("rejected:", exc)
            return
        event = json.loads(body)
        if event["id"] in seen:
            print("duplicate ignored:", event["id"])
        else:
            seen.add(event["id"])
            print("received:", event["type"], event["data"])
        self.send_response(204)
        self.end_headers()

    def log_message(self, *args):
        pass


if __name__ == "__main__":
    print("listening on http://127.0.0.1:9000/hook")
    HTTPServer(("127.0.0.1", 9000), Handler).serve_forever()
