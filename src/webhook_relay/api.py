"""HTTP API: manage subscriptions, publish events, inspect and retry deliveries."""

from __future__ import annotations

import hmac
import json
import secrets
import time
from typing import Any

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Response
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel, Field

from . import __version__
from .config import Config, load_config
from .store import STATUSES, Store, Subscription
from .url_policy import UnsafeTarget, validate_target

MIN_API_KEY_LENGTH = 16


class SubscriptionIn(BaseModel):
    url: str = Field(max_length=2048)
    event_types: list[str] = Field(default_factory=lambda: ["*"], min_length=1, max_length=50)
    secret: str | None = Field(default=None, min_length=16, max_length=128)


class EventIn(BaseModel):
    type: str = Field(min_length=1, max_length=100, pattern=r"^[A-Za-z0-9_.-]+$")
    data: dict[str, Any]


def _public(sub: Subscription, include_secret: bool = False) -> dict[str, Any]:
    out: dict[str, Any] = {
        "id": sub.id,
        "url": sub.url,
        "event_types": sub.event_types,
        "active": sub.active,
        "created_at": sub.created_at,
    }
    if include_secret:
        out["secret"] = sub.secret
    return out


def create_app(store: Store, cfg: Config) -> FastAPI:
    if not cfg.api_key or len(cfg.api_key) < MIN_API_KEY_LENGTH:
        raise ValueError(f"RELAY_API_KEY is required and must be >= {MIN_API_KEY_LENGTH} chars")
    expected_key = cfg.api_key

    def require_key(x_api_key: str | None = Header(default=None)) -> None:
        if x_api_key is None or not hmac.compare_digest(x_api_key, expected_key):
            raise HTTPException(status_code=401, detail="invalid or missing API key")

    app = FastAPI(title="webhook-relay", version=__version__)
    auth = [Depends(require_key)]

    @app.get("/healthz")
    def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/v1/subscriptions", status_code=201, dependencies=auth)
    def create_subscription(body: SubscriptionIn) -> dict[str, Any]:
        try:
            validate_target(body.url, cfg.allow_private_targets)
        except UnsafeTarget as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from None
        secret = body.secret or secrets.token_urlsafe(32)
        sub = store.create_subscription(body.url, body.event_types, secret, time.time())
        return _public(sub, include_secret=True)  # the secret is only ever shown here

    @app.get("/v1/subscriptions", dependencies=auth)
    def list_subscriptions() -> list[dict[str, Any]]:
        return [_public(s) for s in store.list_subscriptions()]

    @app.delete("/v1/subscriptions/{subscription_id}", status_code=204, dependencies=auth)
    def delete_subscription(subscription_id: str) -> Response:
        if not store.deactivate_subscription(subscription_id):
            raise HTTPException(status_code=404, detail="subscription not found")
        return Response(status_code=204)

    @app.post("/v1/events", status_code=202, dependencies=auth)
    def publish_event(
        event: EventIn,
        response: Response,
        idempotency_key: str | None = Header(default=None, max_length=200),
    ) -> dict[str, Any]:
        if len(json.dumps(event.data)) > cfg.max_body_bytes:
            raise HTTPException(status_code=413, detail="event data too large")
        result = store.ingest_event(event.type, event.data, idempotency_key, time.time())
        if not result.created:
            response.status_code = 200  # replay of an earlier request
        return {
            "id": result.event_id,
            "type": result.event_type,
            "created": result.created,
            "deliveries": result.deliveries,
        }

    @app.get("/v1/deliveries", dependencies=auth)
    def list_deliveries(
        status: str | None = Query(default=None),
        event_id: str | None = Query(default=None),
        limit: int = Query(default=50, ge=1, le=200),
    ) -> list[dict[str, Any]]:
        if status is not None and status not in STATUSES:
            raise HTTPException(status_code=422, detail=f"status must be one of {STATUSES}")
        return store.list_deliveries(status=status, event_id=event_id, limit=limit)

    @app.get("/v1/deliveries/{delivery_id}", dependencies=auth)
    def get_delivery(delivery_id: str) -> dict[str, Any]:
        delivery = store.get_delivery(delivery_id)
        if delivery is None:
            raise HTTPException(status_code=404, detail="delivery not found")
        return delivery

    @app.post("/v1/deliveries/{delivery_id}/retry", dependencies=auth)
    def retry_delivery(delivery_id: str) -> dict[str, str]:
        outcome = store.requeue_dead(delivery_id, time.time())
        if outcome == "not_found":
            raise HTTPException(status_code=404, detail="delivery not found")
        if outcome == "not_dead":
            raise HTTPException(status_code=409, detail="only dead deliveries can be retried")
        return {"status": "pending"}

    @app.get("/v1/stats", dependencies=auth)
    def stats() -> dict[str, int]:
        return store.stats()

    @app.get("/metrics", dependencies=auth, response_class=PlainTextResponse)
    def metrics() -> str:
        lines = ["# HELP relay_deliveries Deliveries by status", "# TYPE relay_deliveries gauge"]
        for status, count in store.stats().items():
            lines.append(f'relay_deliveries{{status="{status}"}} {count}')
        return "\n".join(lines) + "\n"

    return app


def create_app_from_env() -> FastAPI:
    cfg = load_config()
    return create_app(Store(cfg.db_path), cfg)
