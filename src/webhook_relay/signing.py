"""HMAC-SHA256 request signing with a timestamp, to prove origin and resist replay.

Header format:  X-Relay-Signature: t=<unix seconds>,v1=<hex digest>
The digest covers  "<t>.<raw body>"  keyed with the subscription secret.
"""

from __future__ import annotations

import hashlib
import hmac
import time

SIGNATURE_VERSION = "v1"


class SignatureError(Exception):
    """Raised when a signature header is malformed, stale or does not match."""


def _mac(secret: str, timestamp: int, body: bytes) -> str:
    message = str(timestamp).encode() + b"." + body
    return hmac.new(secret.encode(), message, hashlib.sha256).hexdigest()


def sign_header(secret: str, body: bytes, timestamp: int | None = None) -> str:
    ts = int(time.time()) if timestamp is None else int(timestamp)
    return f"t={ts},{SIGNATURE_VERSION}={_mac(secret, ts, body)}"


def verify_signature(
    secret: str,
    header: str,
    body: bytes,
    tolerance: float = 300.0,
    now: float | None = None,
) -> int:
    """Return the signed timestamp, or raise SignatureError."""
    fields: dict[str, str] = {}
    for part in header.split(","):
        key, sep, value = part.strip().partition("=")
        if sep:
            fields[key] = value
    try:
        timestamp = int(fields["t"])
        signature = fields[SIGNATURE_VERSION]
    except (KeyError, ValueError):
        raise SignatureError("malformed signature header") from None
    current = time.time() if now is None else now
    if abs(current - timestamp) > tolerance:
        raise SignatureError("timestamp outside tolerance")
    if not hmac.compare_digest(signature, _mac(secret, timestamp, body)):
        raise SignatureError("signature mismatch")
    return timestamp
