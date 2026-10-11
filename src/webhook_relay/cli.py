"""Command line entry point:  webhook-relay api | worker"""

from __future__ import annotations

import argparse
import logging
import signal
import threading

from .config import load_config
from .store import Store
from .worker import UrllibSender, run_forever


def _run_worker() -> None:
    cfg = load_config()
    store = Store(cfg.db_path)
    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    run_forever(store, UrllibSender(cfg.allow_private_targets), cfg, stop)


def _run_api(host: str, port: int) -> None:
    import uvicorn

    uvicorn.run("webhook_relay.api:create_app_from_env", factory=True, host=host, port=port)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="webhook-relay")
    sub = parser.add_subparsers(dest="command", required=True)
    api = sub.add_parser("api", help="run the HTTP API")
    api.add_argument("--host", default="127.0.0.1")
    api.add_argument("--port", type=int, default=8000)
    sub.add_parser("worker", help="run the delivery worker")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    if args.command == "api":
        _run_api(args.host, args.port)
    else:
        _run_worker()


if __name__ == "__main__":
    main()
