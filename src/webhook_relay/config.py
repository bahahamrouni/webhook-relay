from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass


@dataclass(frozen=True)
class Config:
    db_path: str = "relay.db"
    api_key: str | None = None
    max_attempts: int = 6
    base_delay: float = 2.0
    max_delay: float = 300.0
    lease_seconds: float = 60.0
    delivery_timeout: float = 5.0
    poll_interval: float = 1.0
    batch_size: int = 20
    concurrency: int = 4
    max_body_bytes: int = 256 * 1024
    allow_private_targets: bool = False

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")
        if self.base_delay <= 0 or self.max_delay < self.base_delay:
            raise ValueError("require 0 < base_delay <= max_delay")
        if self.lease_seconds <= self.delivery_timeout:
            raise ValueError("lease_seconds must be larger than delivery_timeout")
        if self.concurrency < 1 or self.batch_size < 1:
            raise ValueError("concurrency and batch_size must be at least 1")


def _flag(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "on"}


def load_config(env: Mapping[str, str] | None = None) -> Config:
    """Build a Config from RELAY_* environment variables."""
    e = os.environ if env is None else env
    d = Config()
    return Config(
        db_path=e.get("RELAY_DB_PATH", d.db_path),
        api_key=e.get("RELAY_API_KEY") or None,
        max_attempts=int(e.get("RELAY_MAX_ATTEMPTS", d.max_attempts)),
        base_delay=float(e.get("RELAY_BASE_DELAY", d.base_delay)),
        max_delay=float(e.get("RELAY_MAX_DELAY", d.max_delay)),
        lease_seconds=float(e.get("RELAY_LEASE_SECONDS", d.lease_seconds)),
        delivery_timeout=float(e.get("RELAY_DELIVERY_TIMEOUT", d.delivery_timeout)),
        poll_interval=float(e.get("RELAY_POLL_INTERVAL", d.poll_interval)),
        batch_size=int(e.get("RELAY_BATCH_SIZE", d.batch_size)),
        concurrency=int(e.get("RELAY_CONCURRENCY", d.concurrency)),
        max_body_bytes=int(e.get("RELAY_MAX_BODY_BYTES", d.max_body_bytes)),
        allow_private_targets=_flag(e.get("RELAY_ALLOW_PRIVATE_TARGETS", "0")),
    )
