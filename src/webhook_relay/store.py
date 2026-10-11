"""SQLite persistence: subscriptions, events and the delivery queue.

The `deliveries` table is the queue (a transactional outbox): an event and its
per-subscriber deliveries are written in one transaction, so an accepted event can
never be lost between "stored" and "queued". Workers claim rows with a lease, so a
crashed worker's jobs become claimable again once the lease expires.

Every operation opens its own short-lived connection, which keeps the store safe to
share between threads and between the API and worker processes (SQLite WAL mode).
"""

from __future__ import annotations

import json
import sqlite3
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any

SCHEMA = """
CREATE TABLE IF NOT EXISTS subscriptions (
    id          TEXT PRIMARY KEY,
    url         TEXT NOT NULL,
    secret      TEXT NOT NULL,
    event_types TEXT NOT NULL,
    active      INTEGER NOT NULL DEFAULT 1,
    created_at  REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS events (
    id              TEXT PRIMARY KEY,
    idempotency_key TEXT UNIQUE,
    type            TEXT NOT NULL,
    payload         TEXT NOT NULL,
    created_at      REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS deliveries (
    id               TEXT PRIMARY KEY,
    event_id         TEXT NOT NULL REFERENCES events(id),
    subscription_id  TEXT NOT NULL REFERENCES subscriptions(id),
    status           TEXT NOT NULL
        CHECK (status IN ('pending', 'in_flight', 'delivered', 'dead')),
    attempts         INTEGER NOT NULL DEFAULT 0,
    next_attempt_at  REAL NOT NULL,
    lease_until      REAL,
    last_status_code INTEGER,
    last_error       TEXT,
    created_at       REAL NOT NULL,
    updated_at       REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_deliveries_due ON deliveries(status, next_attempt_at);
CREATE INDEX IF NOT EXISTS idx_deliveries_event ON deliveries(event_id);
"""

STATUSES = ("pending", "in_flight", "delivered", "dead")


def event_type_matches(pattern: str, event_type: str) -> bool:
    """`*` matches everything, `order.*` matches by prefix, anything else must be equal."""
    if pattern == "*":
        return True
    if pattern.endswith(".*"):
        return event_type.startswith(pattern[:-1])
    return pattern == event_type


@dataclass(frozen=True)
class Subscription:
    id: str
    url: str
    secret: str
    event_types: list[str]
    active: bool
    created_at: float


@dataclass(frozen=True)
class IngestResult:
    event_id: str
    event_type: str
    created: bool
    deliveries: int


@dataclass(frozen=True)
class DeliveryJob:
    id: str
    event_id: str
    event_type: str
    payload: str
    event_created_at: float
    subscription_id: str
    url: str
    secret: str
    attempts: int


def _new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex}"


def _subscription(row: sqlite3.Row) -> Subscription:
    return Subscription(
        id=row["id"],
        url=row["url"],
        secret=row["secret"],
        event_types=json.loads(row["event_types"]),
        active=bool(row["active"]),
        created_at=row["created_at"],
    )


class Store:
    def __init__(self, path: str) -> None:
        self.path = path
        conn = sqlite3.connect(path, timeout=30)
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.executescript(SCHEMA)
        finally:
            conn.close()

    # -- connection helpers -------------------------------------------------

    def _open(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=30000")
        return conn

    @contextmanager
    def _read(self) -> Iterator[sqlite3.Connection]:
        conn = self._open()
        try:
            yield conn
        finally:
            conn.close()

    @contextmanager
    def _tx(self) -> Iterator[sqlite3.Connection]:
        """Write transaction. BEGIN IMMEDIATE takes the write lock up front, which makes
        select-then-update sequences (like claiming jobs) atomic across processes."""
        conn = self._open()
        try:
            conn.execute("BEGIN IMMEDIATE")
            yield conn
            conn.execute("COMMIT")
        except BaseException:
            if conn.in_transaction:
                conn.execute("ROLLBACK")
            raise
        finally:
            conn.close()

    # -- subscriptions ------------------------------------------------------

    def create_subscription(
        self, url: str, event_types: list[str], secret: str, now: float
    ) -> Subscription:
        sub_id = _new_id("sub")
        with self._tx() as c:
            c.execute(
                "INSERT INTO subscriptions (id, url, secret, event_types, active, created_at)"
                " VALUES (?, ?, ?, ?, 1, ?)",
                (sub_id, url, secret, json.dumps(event_types), now),
            )
        return Subscription(sub_id, url, secret, list(event_types), True, now)

    def get_subscription(self, sub_id: str) -> Subscription | None:
        with self._read() as c:
            row = c.execute("SELECT * FROM subscriptions WHERE id = ?", (sub_id,)).fetchone()
        return _subscription(row) if row else None

    def list_subscriptions(self, active_only: bool = False) -> list[Subscription]:
        query = "SELECT * FROM subscriptions"
        if active_only:
            query += " WHERE active = 1"
        with self._read() as c:
            rows = c.execute(query + " ORDER BY created_at").fetchall()
        return [_subscription(r) for r in rows]

    def deactivate_subscription(self, sub_id: str) -> bool:
        with self._tx() as c:
            cur = c.execute(
                "UPDATE subscriptions SET active = 0 WHERE id = ? AND active = 1", (sub_id,)
            )
            return cur.rowcount == 1

    # -- events and fan-out -------------------------------------------------

    def ingest_event(
        self,
        event_type: str,
        payload: dict[str, Any],
        idempotency_key: str | None,
        now: float,
    ) -> IngestResult:
        with self._tx() as c:
            if idempotency_key is not None:
                existing = c.execute(
                    "SELECT id, type FROM events WHERE idempotency_key = ?", (idempotency_key,)
                ).fetchone()
                if existing:
                    count = c.execute(
                        "SELECT COUNT(*) FROM deliveries WHERE event_id = ?", (existing["id"],)
                    ).fetchone()[0]
                    return IngestResult(existing["id"], existing["type"], False, count)
            event_id = _new_id("evt")
            c.execute(
                "INSERT INTO events (id, idempotency_key, type, payload, created_at)"
                " VALUES (?, ?, ?, ?, ?)",
                (event_id, idempotency_key, event_type, json.dumps(payload), now),
            )
            count = 0
            for row in c.execute("SELECT id, event_types FROM subscriptions WHERE active = 1"):
                patterns = json.loads(row["event_types"])
                if any(event_type_matches(p, event_type) for p in patterns):
                    c.execute(
                        "INSERT INTO deliveries (id, event_id, subscription_id, status, attempts,"
                        " next_attempt_at, created_at, updated_at)"
                        " VALUES (?, ?, ?, 'pending', 0, ?, ?, ?)",
                        (_new_id("dlv"), event_id, row["id"], now, now, now),
                    )
                    count += 1
            return IngestResult(event_id, event_type, True, count)

    # -- queue operations ---------------------------------------------------

    def claim_due(self, now: float, limit: int, lease_seconds: float) -> list[DeliveryJob]:
        """Atomically lease up to `limit` due deliveries. Expired leases are reclaimed."""
        jobs: list[DeliveryJob] = []
        with self._tx() as c:
            due = c.execute(
                "SELECT id FROM deliveries"
                " WHERE (status = 'pending' AND next_attempt_at <= ?)"
                "    OR (status = 'in_flight' AND lease_until <= ?)"
                " ORDER BY next_attempt_at LIMIT ?",
                (now, now, limit),
            ).fetchall()
            for entry in due:
                row = c.execute(
                    "SELECT d.id, d.event_id, d.subscription_id, d.attempts, e.type,"
                    " e.payload, e.created_at AS event_created_at, s.url, s.secret, s.active"
                    " FROM deliveries d"
                    " JOIN events e ON e.id = d.event_id"
                    " JOIN subscriptions s ON s.id = d.subscription_id"
                    " WHERE d.id = ?",
                    (entry["id"],),
                ).fetchone()
                if not row["active"]:
                    c.execute(
                        "UPDATE deliveries SET status = 'dead', last_error = ?, lease_until = NULL,"
                        " updated_at = ? WHERE id = ?",
                        ("subscription inactive", now, row["id"]),
                    )
                    continue
                c.execute(
                    "UPDATE deliveries SET status = 'in_flight', attempts = attempts + 1,"
                    " lease_until = ?, updated_at = ? WHERE id = ?",
                    (now + lease_seconds, now, row["id"]),
                )
                jobs.append(
                    DeliveryJob(
                        id=row["id"],
                        event_id=row["event_id"],
                        event_type=row["type"],
                        payload=row["payload"],
                        event_created_at=row["event_created_at"],
                        subscription_id=row["subscription_id"],
                        url=row["url"],
                        secret=row["secret"],
                        attempts=row["attempts"] + 1,
                    )
                )
        return jobs

    def mark_delivered(self, delivery_id: str, status_code: int, now: float) -> None:
        with self._tx() as c:
            c.execute(
                "UPDATE deliveries SET status = 'delivered', last_status_code = ?,"
                " last_error = NULL, lease_until = NULL, updated_at = ? WHERE id = ?",
                (status_code, now, delivery_id),
            )

    def mark_retry(
        self,
        delivery_id: str,
        now: float,
        retry_at: float,
        status_code: int | None,
        error: str | None,
    ) -> None:
        with self._tx() as c:
            c.execute(
                "UPDATE deliveries SET status = 'pending', next_attempt_at = ?,"
                " last_status_code = ?, last_error = ?, lease_until = NULL, updated_at = ?"
                " WHERE id = ?",
                (retry_at, status_code, error, now, delivery_id),
            )

    def mark_dead(
        self, delivery_id: str, now: float, status_code: int | None, error: str | None
    ) -> None:
        with self._tx() as c:
            c.execute(
                "UPDATE deliveries SET status = 'dead', last_status_code = ?, last_error = ?,"
                " lease_until = NULL, updated_at = ? WHERE id = ?",
                (status_code, error, now, delivery_id),
            )

    def requeue_dead(self, delivery_id: str, now: float) -> str:
        """Reset a dead delivery. Returns 'requeued', 'not_dead' or 'not_found'."""
        with self._tx() as c:
            row = c.execute(
                "SELECT status FROM deliveries WHERE id = ?", (delivery_id,)
            ).fetchone()
            if row is None:
                return "not_found"
            if row["status"] != "dead":
                return "not_dead"
            c.execute(
                "UPDATE deliveries SET status = 'pending', attempts = 0, next_attempt_at = ?,"
                " last_error = NULL, updated_at = ? WHERE id = ?",
                (now, now, delivery_id),
            )
            return "requeued"

    # -- inspection ---------------------------------------------------------

    def get_delivery(self, delivery_id: str) -> dict[str, Any] | None:
        with self._read() as c:
            row = c.execute("SELECT * FROM deliveries WHERE id = ?", (delivery_id,)).fetchone()
        return dict(row) if row else None

    def list_deliveries(
        self, status: str | None = None, event_id: str | None = None, limit: int = 50
    ) -> list[dict[str, Any]]:
        clauses, params = [], []
        if status is not None:
            clauses.append("status = ?")
            params.append(status)
        if event_id is not None:
            clauses.append("event_id = ?")
            params.append(event_id)
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        with self._read() as c:
            rows = c.execute(
                f"SELECT * FROM deliveries{where} ORDER BY created_at DESC, id LIMIT ?",
                (*params, limit),
            ).fetchall()
        return [dict(r) for r in rows]

    def stats(self) -> dict[str, int]:
        counts = dict.fromkeys(STATUSES, 0)
        with self._read() as c:
            for row in c.execute("SELECT status, COUNT(*) AS n FROM deliveries GROUP BY status"):
                counts[row["status"]] = row["n"]
        return counts
