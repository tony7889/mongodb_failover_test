from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class AppConfig:
    uri: str
    database: str = "dbFailoverTest"
    collection: str = "probes"
    interval_seconds: float = 0.5
    duration_seconds: float = 120.0
    server_selection_timeout_ms: int = 30_000
    connect_timeout_ms: int = 10_000
    socket_timeout_ms: int = 30_000
    step_down: bool = False
    step_down_delay_seconds: float = 10.0
    step_down_seconds: int = 60
    mongosh_binary: str = "mongosh"

    @classmethod
    def from_env(cls) -> "AppConfig":
        uri = os.getenv("MONGODB_URI")
        if not uri:
            raise ValueError("MONGODB_URI must be set or passed with --uri")
        return cls(
            uri=uri,
            database=os.getenv("MONGODB_DATABASE", "dbFailoverTest"),
            collection=os.getenv("MONGODB_COLLECTION", "probes"),
            interval_seconds=float(os.getenv("FAILOVER_INTERVAL_SECONDS", "0.5")),
            duration_seconds=float(os.getenv("FAILOVER_DURATION_SECONDS", "120")),
            server_selection_timeout_ms=int(os.getenv("MONGODB_SERVER_SELECTION_TIMEOUT_MS", "30000")),
            connect_timeout_ms=int(os.getenv("MONGODB_CONNECT_TIMEOUT_MS", "10000")),
            socket_timeout_ms=int(os.getenv("MONGODB_SOCKET_TIMEOUT_MS", "30000")),
            step_down=os.getenv("FAILOVER_STEP_DOWN", "false").lower() == "true",
            step_down_delay_seconds=float(os.getenv("FAILOVER_STEP_DOWN_DELAY_SECONDS", "10")),
            step_down_seconds=int(os.getenv("FAILOVER_STEP_DOWN_SECONDS", "60")),
            mongosh_binary=os.getenv("MONGOSH_BINARY", "mongosh"),
        )
