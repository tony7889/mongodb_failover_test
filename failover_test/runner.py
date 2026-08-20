from __future__ import annotations

import json
import logging
import subprocess
import threading
import time
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from collections import Counter
from typing import Any

from pymongo import MongoClient, WriteConcern, monitoring
from pymongo.errors import PyMongoError

from .config import AppConfig

LOGGER = logging.getLogger("mongodb_failover_test")


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class EventRecorder(monitoring.TopologyListener, monitoring.ServerListener, monitoring.ServerHeartbeatListener):
    """Captures driver topology and heartbeat events for the test report."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.events: list[dict[str, Any]] = []

    def _record(self, event_type: str, **values: Any) -> None:
        with self._lock:
            self.events.append({"timestamp": utc_now(), "event": event_type, **values})

    def opened(self, event: Any) -> None:
        if hasattr(event, "topology_id"):
            self._record("topology_opened", topology_id=str(event.topology_id))
        elif hasattr(event, "server_address"):
            self._record("server_opened", address=str(event.server_address))

    def closed(self, event: Any) -> None:
        if hasattr(event, "topology_id"):
            self._record("topology_closed", topology_id=str(event.topology_id))
        elif hasattr(event, "server_address"):
            self._record("server_closed", address=str(event.server_address))

    def description_changed(self, event: Any) -> None:
        new_description = getattr(event, "new_description", None)
        if hasattr(new_description, "topology_type_name"):
            self._record(
                "topology_description_changed",
                topology_type=new_description.topology_type_name,
            )
            LOGGER.info("MongoDB topology changed: %s", new_description.topology_type_name)
        else:
            server_type = getattr(new_description, "server_type_name", str(new_description))
            self._record(
                "server_description_changed",
                address=str(getattr(event, "server_address", "")),
                server_type=server_type,
            )
            LOGGER.info("MongoDB server changed: %s -> %s", getattr(event, "server_address", ""), server_type)

    def started(self, event: Any) -> None:
        self._record("heartbeat_started", address=str(getattr(event, "connection_id", "")))

    def succeeded(self, event: Any) -> None:
        self._record(
            "heartbeat_succeeded",
            address=str(getattr(event, "connection_id", "")),
            duration_ms=getattr(event, "duration_millis", None),
        )

    def failed(self, event: Any) -> None:
        self._record(
            "heartbeat_failed",
            address=str(getattr(event, "connection_id", "")),
            duration_ms=getattr(event, "duration_millis", None),
            error=str(getattr(event, "reply", "")),
        )


@dataclass
class RunSummary:
    run_id: str
    started_at: str
    finished_at: str | None = None
    duration_seconds: float = 0.0
    total_operations: int = 0
    successful_operations: int = 0
    failed_operations: int = 0
    first_failure_at: str | None = None
    recovery_at: str | None = None
    recovery_seconds: float | None = None
    step_down_requested: bool = False
    step_down_result: str | None = None
    errors: list[dict[str, Any]] = field(default_factory=list)
    topology_events: list[dict[str, Any]] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        # A primary election can produce a transient operation error. Treat the
        # run as successful when operations resume after the first failure.
        return self.successful_operations > 0 and (
            self.failed_operations == 0 or self.recovery_at is not None
        )

    def to_dict(self) -> dict[str, Any]:
        result = asdict(self)
        result["passed"] = self.passed
        return result

    def to_report(self, max_errors: int = 5, related_events_per_error: int = 5) -> dict[str, Any]:
        error_counts = Counter(error.get("type", "UnknownError") for error in self.errors)

        important_types = {
            "topology_opened",
            "topology_closed",
            "server_opened",
            "server_closed",
            "topology_description_changed",
            "server_description_changed",
            "heartbeat_failed",
        }

        def event_message(event: dict[str, Any]) -> str:
            if event.get("error"):
                return str(event.get("error"))
            if event.get("topology_type"):
                return f"topology={event.get('topology_type')}"
            if event.get("server_type"):
                return f"server={event.get('address', '')} type={event.get('server_type')}"
            if event.get("address"):
                return f"address={event.get('address')}"
            return ""

        def related_events(error_timestamp: str | None) -> list[dict[str, Any]]:
            if not error_timestamp:
                return []

            candidates: list[dict[str, Any]] = []
            for event in self.topology_events:
                if event.get("event") not in important_types:
                    continue
                event_ts = event.get("timestamp")
                if not isinstance(event_ts, str):
                    continue
                if event_ts <= error_timestamp:
                    candidates.append(event)

            selected = candidates[-related_events_per_error:]
            return [
                {
                    "time": event.get("timestamp"),
                    "event": event.get("event"),
                    "message": event_message(event),
                }
                for event in selected
            ]

        errors: list[dict[str, Any]] = []
        for error in self.errors[:max_errors]:
            message = str(error.get("message", ""))
            error_time = error.get("timestamp")
            errors.append(
                {
                    "time": error_time,
                    "type": error.get("type"),
                    "message": message if len(message) <= 300 else f"{message[:300]}...",
                    "related_events": related_events(error_time if isinstance(error_time, str) else None),
                }
            )

        return {
            "summary": {
                "run_id": self.run_id,
                "started_at": self.started_at,
                "finished_at": self.finished_at,
                "duration_seconds": round(self.duration_seconds, 2),
                "total_operations": self.total_operations,
                "successful_operations": self.successful_operations,
                "failed_operations": self.failed_operations,
                "first_failure_at": self.first_failure_at,
                "recovery_at": self.recovery_at,
                "recovery_seconds": self.recovery_seconds,
                "step_down_requested": self.step_down_requested,
                "step_down_result": self.step_down_result,
                "passed": self.passed,
            },
            "errors": {
                "total": len(self.errors),
                "types": dict(error_counts),
                "items": errors,
            },
        }


class FailoverRunner:
    def __init__(self, config: AppConfig) -> None:
        self.config = config

    def _client(self, listener: EventRecorder) -> MongoClient:
        return MongoClient(
            self.config.uri,
            appname="mongodb-failover-test",
            retryWrites=True,
            retryReads=True,
            serverSelectionTimeoutMS=self.config.server_selection_timeout_ms,
            connectTimeoutMS=self.config.connect_timeout_ms,
            socketTimeoutMS=self.config.socket_timeout_ms,
            event_listeners=[listener],
        )

    def _run_mongosh_stepdown(self) -> str:
        command = f"rs.stepDown({self.config.step_down_seconds})"
        try:
            completed = subprocess.run(
                [self.config.mongosh_binary, self.config.uri, "--quiet", "--eval", command],
                capture_output=True,
                text=True,
                timeout=max(30, self.config.step_down_seconds + 15),
                check=False,
            )
            output = (completed.stdout + completed.stderr).strip()
            if completed.returncode == 0:
                return f"completed: {output}" if output else "completed"
            return f"failed with exit code {completed.returncode}: {output}"
        except (OSError, subprocess.SubprocessError) as exc:
            return f"failed to invoke mongosh: {exc}"

    def run(self) -> RunSummary:
        run_id = str(uuid.uuid4())
        summary = RunSummary(
            run_id=run_id,
            started_at=utc_now(),
            step_down_requested=self.config.step_down,
        )
        listener = EventRecorder()
        client = self._client(listener)
        collection = client.get_database(self.config.database).get_collection(
            self.config.collection,
            write_concern=WriteConcern(w="majority"),
        )
        start_monotonic = time.monotonic()
        failure_started: float | None = None
        stepdown_started = False
        LOGGER.info("Starting failover test %s", run_id)

        try:
            client.admin.command("ping")
            while time.monotonic() - start_monotonic < self.config.duration_seconds:
                elapsed = time.monotonic() - start_monotonic
                if self.config.step_down and not stepdown_started and elapsed >= self.config.step_down_delay_seconds:
                    stepdown_started = True
                    LOGGER.warning("Invoking mongosh rs.stepDown(%s)", self.config.step_down_seconds)
                    summary.step_down_result = self._run_mongosh_stepdown()

                token = f"{run_id}:{summary.total_operations}"
                summary.total_operations += 1
                operation_started = time.monotonic()
                try:
                    collection.update_one(
                        {"_id": token},
                        {"$set": {"run_id": run_id, "updated_at": utc_now()}},
                        upsert=True,
                    )
                    collection.find_one({"_id": token})
                    summary.successful_operations += 1
                    if failure_started is not None and summary.recovery_at is None:
                        summary.recovery_at = utc_now()
                        summary.recovery_seconds = time.monotonic() - failure_started
                except PyMongoError as exc:
                    summary.failed_operations += 1
                    if summary.first_failure_at is None:
                        summary.first_failure_at = utc_now()
                        failure_started = time.monotonic()
                    if len(summary.errors) < 25:
                        summary.errors.append(
                            {
                                "timestamp": utc_now(),
                                "type": type(exc).__name__,
                                "message": str(exc),
                                "duration_ms": round((time.monotonic() - operation_started) * 1000, 2),
                            }
                        )
                    LOGGER.debug("Operation failed: %s", exc)

                time.sleep(max(0.0, self.config.interval_seconds))
        except PyMongoError as exc:
            summary.failed_operations += 1
            summary.first_failure_at = summary.first_failure_at or utc_now()
            if len(summary.errors) < 25:
                summary.errors.append(
                    {
                        "timestamp": utc_now(),
                        "type": type(exc).__name__,
                        "message": str(exc),
                        "duration_ms": None,
                    }
                )
            LOGGER.debug("Initial connection failed: %s", exc)
        finally:
            summary.finished_at = utc_now()
            summary.duration_seconds = time.monotonic() - start_monotonic
            summary.topology_events = listener.events[-200:]
            client.close()
            LOGGER.info(
                "Finished failover test %s passed=%s ops=%s success=%s failed=%s",
                run_id,
                summary.passed,
                summary.total_operations,
                summary.successful_operations,
                summary.failed_operations,
            )

        return summary
