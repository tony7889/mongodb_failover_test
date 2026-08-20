from __future__ import annotations

import argparse
import json
import logging
import os

import uvicorn

from .config import AppConfig
from .runner import FailoverRunner


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="MongoDB replica-set client failover test")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="run the synthetic read/write failover test")
    run_parser.add_argument("--uri", default=os.getenv("MONGODB_URI"), help="MongoDB URI; defaults to MONGODB_URI")
    run_parser.add_argument("--db", default=os.getenv("MONGODB_DATABASE", "dbFailoverTest"))
    run_parser.add_argument("--collection", default=os.getenv("MONGODB_COLLECTION", "probes"))
    run_parser.add_argument("--duration", type=float, default=120.0, help="test duration in seconds")
    run_parser.add_argument("--interval", type=float, default=0.5, help="seconds between operations")
    run_parser.add_argument("--step-down", action="store_true", help="invoke mongosh rs.stepDown after the delay")
    run_parser.add_argument("--step-down-delay", type=float, default=10.0)
    run_parser.add_argument("--step-down-seconds", type=int, default=60)
    run_parser.add_argument("--mongosh-binary", default="mongosh")
    run_parser.add_argument("--server-selection-timeout-ms", type=int, default=30_000)
    run_parser.add_argument("--full-output", action="store_true", help="print full raw JSON output")
    run_parser.add_argument("--log-level", default="WARNING", help="logging level (DEBUG, INFO, WARNING, ERROR)")

    serve_parser = subparsers.add_parser("serve", help="start the FastAPI service")
    serve_parser.add_argument("--host", default="0.0.0.0")
    serve_parser.add_argument("--port", type=int, default=8080)
    serve_parser.add_argument("--reload", action="store_true")
    serve_parser.add_argument("--log-level", default="WARNING", help="logging level (DEBUG, INFO, WARNING, ERROR)")
    return parser


def config_from_args(args: argparse.Namespace) -> AppConfig:
    uri = (args.uri or "").strip()
    if not uri:
        raise SystemExit(
            "MONGODB_URI or --uri is required (set it in the same shell running this command)"
        )
    return AppConfig(
        uri=uri,
        database=args.db,
        collection=args.collection,
        duration_seconds=args.duration,
        interval_seconds=args.interval,
        step_down=args.step_down,
        step_down_delay_seconds=args.step_down_delay,
        step_down_seconds=args.step_down_seconds,
        mongosh_binary=args.mongosh_binary,
        server_selection_timeout_ms=args.server_selection_timeout_ms,
    )


def main() -> None:
    args = build_parser().parse_args()
    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.WARNING),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    if args.command == "run":
        summary = FailoverRunner(config_from_args(args)).run()
        payload = summary.to_dict() if args.full_output else summary.to_report()
        print(json.dumps(payload, indent=2, default=str))
        raise SystemExit(0 if summary.passed else 1)

    uvicorn.run("failover_test.api:app", host=args.host, port=args.port, reload=args.reload)


if __name__ == "__main__":
    main()
