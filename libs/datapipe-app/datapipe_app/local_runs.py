"""Minimal in-memory pipeline run tracker for local Ops API v1alpha3.

Full observability DB is not required; this gives LocalShell run history,
start/stop, and buffered logs until a richer store is wired.
"""

from __future__ import annotations

import threading
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple

Labels = List[Tuple[str, str]]

_STOP_REASON = "Stopped by user"


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def normalize_labels(raw: Optional[Sequence[Sequence[str]]]) -> Labels:
    result: Labels = []
    for item in raw or []:
        if len(item) >= 2:
            result.append((str(item[0]), str(item[1])))
    return result


def trigger_from_labels(labels: Labels) -> str:
    if not labels:
        return "api:pipeline"
    stage = next((value for key, value in labels if key == "stage"), None)
    if stage:
        return f"api:stage:{stage}"
    return "api"


def derive_run_scope(labels: Labels) -> Dict[str, Any]:
    if not labels:
        return {
            "run_scope": "full_pipeline",
            "target_labels": [],
            "target_label_display": "all labels",
        }
    stage = next((value for key, value in labels if key == "stage"), None)
    if stage:
        return {
            "run_scope": "stage_run",
            "target_labels": [[key, value] for key, value in labels],
            "target_label_display": stage,
        }
    return {
        "run_scope": "label_run",
        "target_labels": [[key, value] for key, value in labels],
        "target_label_display": labels[0][1],
    }


@dataclass
class LocalRun:
    run_id: str
    pipeline_id: str
    status: str
    trigger: str = "api:pipeline"
    labels: Labels = field(default_factory=list)
    started_at: datetime = field(default_factory=_utcnow)
    finished_at: Optional[datetime] = None
    error: Optional[str] = None
    steps: List[Dict[str, Any]] = field(default_factory=list)
    logs: List[Dict[str, Any]] = field(default_factory=list)
    cancel_requested: bool = False
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def append_log(self, level: str, message: str) -> None:
        with self._lock:
            seq = len(self.logs) + 1
            self.logs.append(
                {
                    "seq": seq,
                    "logged_at": _utcnow().isoformat(),
                    "level": level,
                    "message": message,
                }
            )

    def get_logs(self, after: int = 0, limit: int = 500) -> List[Dict[str, Any]]:
        with self._lock:
            lines = [ln for ln in self.logs if ln["seq"] > after]
            return lines[:limit]

    def max_log_seq(self) -> int:
        with self._lock:
            return self.logs[-1]["seq"] if self.logs else 0

    def to_detail(self) -> Dict[str, Any]:
        scope = derive_run_scope(self.labels)
        with self._lock:
            steps = list(self.steps)
            return {
                "run_id": self.run_id,
                "pipeline_id": self.pipeline_id,
                "status": self.status,
                "started_at": self.started_at.isoformat() if self.started_at else None,
                "finished_at": self.finished_at.isoformat() if self.finished_at else None,
                "error": self.error,
                "trigger": self.trigger,
                **scope,
                "steps": steps,
            }

    def to_list_row(self) -> Dict[str, Any]:
        scope = derive_run_scope(self.labels)
        duration_s: Optional[int] = None
        with self._lock:
            if self.started_at and self.finished_at:
                duration_s = int((self.finished_at - self.started_at).total_seconds())
            return {
                "run_id": self.run_id,
                "pipeline_id": self.pipeline_id,
                "status": self.status,
                "scope": scope["run_scope"],
                "target_label": scope.get("target_label_display"),
                "started_at": self.started_at.isoformat() if self.started_at else None,
                "finished_at": self.finished_at.isoformat() if self.finished_at else None,
                "duration_s": duration_s,
                "trigger": self.trigger,
            }

    def to_summary(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "run_id": self.run_id,
                "status": self.status,
                "started_at": self.started_at.isoformat() if self.started_at else None,
                "finished_at": self.finished_at.isoformat() if self.finished_at else None,
                "trigger": self.trigger,
            }


class LocalRunStore:
    def __init__(self, pipeline_id: str = "local") -> None:
        self.pipeline_id = pipeline_id
        self._runs: Dict[str, LocalRun] = {}
        self._lock = threading.Lock()

    def create(
        self,
        *,
        labels: Optional[Labels] = None,
        trigger: Optional[str] = None,
        step_names: Optional[Sequence[str]] = None,
    ) -> LocalRun:
        labels_norm = list(labels or [])
        run = LocalRun(
            run_id=f"run_{uuid.uuid4().hex[:12]}",
            pipeline_id=self.pipeline_id,
            status="running",
            trigger=trigger or trigger_from_labels(labels_norm),
            labels=labels_norm,
            steps=[
                {
                    "step_name": name,
                    "status": "pending",
                    "started_at": None,
                    "finished_at": None,
                    "processed": None,
                    "total": None,
                    "error": None,
                }
                for name in (step_names or [])
            ],
        )
        run.append_log("info", "Run started")
        with self._lock:
            self._runs[run.run_id] = run
        return run

    def get(self, run_id: str) -> Optional[LocalRun]:
        with self._lock:
            return self._runs.get(run_id)

    def list_runs(
        self,
        *,
        status: Optional[str] = None,
        limit: int = 25,
        offset: int = 0,
    ) -> Tuple[List[LocalRun], int]:
        with self._lock:
            rows = sorted(self._runs.values(), key=lambda r: r.started_at, reverse=True)
        if status:
            rows = [r for r in rows if r.status == status]
        total = len(rows)
        return rows[offset : offset + limit], total

    def recent(self, limit: int = 10) -> List[LocalRun]:
        rows, _ = self.list_runs(limit=limit, offset=0)
        return rows

    def finish(
        self,
        run_id: str,
        *,
        status: str,
        error: Optional[str] = None,
    ) -> Optional[LocalRun]:
        run = self.get(run_id)
        if run is None:
            return None
        with run._lock:
            if run.status in ("succeeded", "failed", "interrupted"):
                return run
            run.status = status
            run.error = error
            run.finished_at = _utcnow()
            for step in run.steps:
                if step.get("status") in (None, "pending", "running"):
                    step["status"] = status if status != "succeeded" else "succeeded"
                    step["finished_at"] = run.finished_at.isoformat()
        if error:
            run.append_log("error", error)
        else:
            run.append_log("info", f"Run finished with status={status}")
        return run

    def request_stop(self, run_id: str) -> Tuple[Optional[LocalRun], bool]:
        """Best-effort stop: mark interrupted. Returns (run, was_running)."""
        run = self.get(run_id)
        if run is None:
            return None, False
        with run._lock:
            if run.status != "running":
                return run, False
            run.cancel_requested = True
            run.status = "interrupted"
            run.error = _STOP_REASON
            run.finished_at = _utcnow()
            for step in run.steps:
                if step.get("status") in (None, "pending", "running"):
                    step["status"] = "interrupted"
                    step["error"] = _STOP_REASON
                    step["finished_at"] = run.finished_at.isoformat()
        run.append_log("warning", _STOP_REASON)
        return run, True
