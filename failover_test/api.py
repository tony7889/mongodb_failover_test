from __future__ import annotations

import os
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from .config import AppConfig
from .runner import FailoverRunner, RunSummary

app = FastAPI(title="MongoDB Failover Test", version="0.1.0")
_executor = ThreadPoolExecutor(max_workers=2)
_runs: dict[str, dict[str, Any]] = {}
_runs_lock = threading.Lock()


class RunRequest(BaseModel):
    duration_seconds: float = Field(default=120, gt=0, le=86_400)
    interval_seconds: float = Field(default=0.5, gt=0, le=3_600)
    step_down: bool = False
    step_down_delay_seconds: float = Field(default=10, ge=0)
    step_down_seconds: int = Field(default=60, ge=5, le=3_600)


def base_config() -> AppConfig:
    return AppConfig.from_env()


def execute(run_id: str, request: RunRequest) -> None:
    try:
        base = base_config()
        config = AppConfig(
            **{
                **base.__dict__,
                "duration_seconds": request.duration_seconds,
                "interval_seconds": request.interval_seconds,
                "step_down": request.step_down,
                "step_down_delay_seconds": request.step_down_delay_seconds,
                "step_down_seconds": request.step_down_seconds,
            }
        )
        summary = FailoverRunner(config).run()
        with _runs_lock:
            _runs[run_id] = {"status": "completed", "summary": summary.to_report()}
    except Exception as exc:
        with _runs_lock:
            _runs[run_id] = {"status": "failed", "error": f"{type(exc).__name__}: {exc}"}


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/runs", status_code=202)
def start_run(request: RunRequest) -> dict[str, str]:
    run_id = str(uuid.uuid4())
    with _runs_lock:
        _runs[run_id] = {"status": "running"}
    _executor.submit(execute, run_id, request)
    return {"run_id": run_id, "status": "running"}


@app.get("/runs/{run_id}")
def get_run(run_id: str) -> dict[str, Any]:
    with _runs_lock:
        result = _runs.get(run_id)
    if result is None:
        raise HTTPException(status_code=404, detail="run not found")
    return {"run_id": run_id, **result}
