"""Delivery worker: claim due deliveries, send them signed, record the outcome."""

from __future__ import annotations

import json
import logging
import threading
import time
import urllib.error
import urllib.request
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Protocol

from .backoff import compute_delay
from .config import Config
from .signing import sign_header
from .store import DeliveryJob, Store
from .url_policy import UnsafeTarget, validate_target

log = logging.getLogger("webhook_relay.worker")

USER_AGENT = "webhook-relay/0.1"
RETRYABLE_STATUS = {408, 425, 429}


@dataclass(frozen=True)
class SendResult:
    status_code: int | None
    error: str | None = None
    permanent: bool = False


class Sender(Protocol):
    def send(
        self, url: str, headers: dict[str, str], body: bytes, timeout: float
    ) -> SendResult: ...


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    """Never follow redirects: a 302 to an internal address would bypass the URL policy."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):  # type: ignore[override]
        return None


class UrllibSender:
    def __init__(self, allow_private: bool = False) -> None:
        self.allow_private = allow_private
        self._opener = urllib.request.build_opener(_NoRedirect)

    def send(
        self, url: str, headers: dict[str, str], body: bytes, timeout: float
    ) -> SendResult:
        try:
            validate_target(url, self.allow_private)
        except UnsafeTarget as exc:
            return SendResult(None, f"unsafe target: {exc}", permanent=True)
        request = urllib.request.Request(url, data=body, headers=headers, method="POST")
        try:
            with self._opener.open(request, timeout=timeout) as response:
                return SendResult(response.status)
        except urllib.error.HTTPError as exc:
            return SendResult(exc.code, f"HTTP {exc.code}")
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            return SendResult(None, f"{type(exc).__name__}: {exc}")


def classify(result: SendResult) -> str:
    """Return 'delivered', 'retry' or 'dead' for a send attempt."""
    if result.permanent:
        return "dead"
    code = result.status_code
    if code is not None and 200 <= code < 300:
        return "delivered"
    if code is None or code >= 500 or code in RETRYABLE_STATUS:
        return "retry"
    return "dead"


def build_request(job: DeliveryJob, now: float) -> tuple[dict[str, str], bytes]:
    created = datetime.fromtimestamp(job.event_created_at, timezone.utc).isoformat()
    envelope = {
        "id": job.event_id,
        "type": job.event_type,
        "created_at": created,
        "data": json.loads(job.payload),
    }
    body = json.dumps(envelope, separators=(",", ":"), sort_keys=True).encode()
    headers = {
        "Content-Type": "application/json",
        "User-Agent": USER_AGENT,
        "X-Relay-Event-Id": job.event_id,
        "X-Relay-Delivery-Id": job.id,
        "X-Relay-Attempt": str(job.attempts),
        "X-Relay-Signature": sign_header(job.secret, body, int(now)),
    }
    return headers, body


def _handle(
    job: DeliveryJob, store: Store, sender: Sender, cfg: Config, now: float, rng: object | None
) -> None:
    if job.attempts > cfg.max_attempts:
        store.mark_dead(job.id, now, None, "attempt limit exceeded (worker lease expired)")
        return
    headers, body = build_request(job, now)
    result = sender.send(job.url, headers, body, cfg.delivery_timeout)
    outcome = classify(result)
    if outcome == "delivered":
        store.mark_delivered(job.id, result.status_code or 0, now)
        return
    error = result.error or f"HTTP {result.status_code}"
    if outcome == "dead":
        store.mark_dead(job.id, now, result.status_code, error)
        return
    if job.attempts >= cfg.max_attempts:
        store.mark_dead(job.id, now, result.status_code, f"{error} (attempts exhausted)")
        return
    delay = compute_delay(job.attempts, cfg.base_delay, cfg.max_delay, rng)  # type: ignore[arg-type]
    store.mark_retry(job.id, now, now + delay, result.status_code, error)


def process_batch(
    store: Store,
    sender: Sender,
    cfg: Config,
    now: float | None = None,
    rng: object | None = None,
) -> int:
    """Claim and process one batch. Returns the number of deliveries attempted."""
    current = time.time() if now is None else now
    jobs = store.claim_due(current, cfg.batch_size, cfg.lease_seconds)
    if not jobs:
        return 0
    if cfg.concurrency == 1 or len(jobs) == 1:
        for job in jobs:
            _handle(job, store, sender, cfg, current, rng)
    else:
        with ThreadPoolExecutor(max_workers=cfg.concurrency) as pool:
            futures = [pool.submit(_handle, j, store, sender, cfg, current, rng) for j in jobs]
            for future in futures:
                future.result()
    return len(jobs)


def run_forever(
    store: Store,
    sender: Sender,
    cfg: Config,
    stop: threading.Event,
    clock: Callable[[], float] = time.time,
) -> None:
    log.info("worker started (concurrency=%d, batch=%d)", cfg.concurrency, cfg.batch_size)
    while not stop.is_set():
        try:
            handled = process_batch(store, sender, cfg, now=clock())
        except Exception:  # keep the loop alive; the lease makes the work reclaimable
            log.exception("batch failed")
            handled = 0
        if handled == 0:
            stop.wait(cfg.poll_interval)
    log.info("worker stopped")
