from __future__ import annotations

import hashlib
import os
import socket
import threading
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from types import TracebackType
from typing import Any, Optional, Type

import pandas as pd
from datapipe.datatable import DataTable
from datapipe.types import IndexDF

from datapipe_ml.training.specs import (
    LocalTrainingLauncher,
    SkyVastTrainingLauncherConfig,
    TrainingLauncherConfig,
    TrainingResumeConfig,
)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_iso(dt: Optional[datetime] = None) -> str:
    return (dt or utc_now()).isoformat()


def parse_duration(value: str) -> timedelta:
    value = value.strip()
    if not value:
        raise ValueError("Duration must not be empty")
    unit = value[-1]
    amount = float(value[:-1])
    if unit == "s":
        return timedelta(seconds=amount)
    if unit == "m":
        return timedelta(minutes=amount)
    if unit == "h":
        return timedelta(hours=amount)
    if unit == "d":
        return timedelta(days=amount)
    raise ValueError(f"Unsupported duration: {value!r}")


def parse_datetime(value: Any) -> Optional[datetime]:
    if value is None or pd.isna(value):
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    return datetime.fromisoformat(str(value))


def owner_id() -> str:
    return f"{socket.gethostname()}:{os.getpid()}"


def training_run_key(
    *,
    idx: IndexDF,
    model_other_primary_keys: list[str],
    frozen_dataset_id_col: str,
    frozen_dataset_id: Any,
    train_config_id_col: str,
    train_config_id: Any,
    model_suffix: str,
) -> str:
    parts: list[str] = []
    if len(idx):
        index_row = idx.iloc[0]
        for key in model_other_primary_keys:
            parts.append(f"{key}={index_row[key]}")
    parts.extend(
        [
            f"{frozen_dataset_id_col}={frozen_dataset_id}",
            f"{train_config_id_col}={train_config_id}",
            f"model_suffix={model_suffix}",
        ]
    )
    payload = "|".join(parts)
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()[:24]


@dataclass
class TrainingLauncherRunState:
    launcher_type: str
    started_at: Optional[str] = None
    last_seen_at: Optional[str] = None


@dataclass
class LocalTrainingRunState(TrainingLauncherRunState):
    launcher_type: str = "local"


@dataclass
class SkyVastTrainingRunState(TrainingLauncherRunState):
    launcher_type: str = "sky_vast"
    cluster_name: str = ""
    remote_root: str = "/workspace/datapipe_ml"
    remote_output_dir: str = "/workspace/datapipe_ml/output/models"
    remote_signals_dir: str = "/workspace/datapipe_ml/signals"


def launcher_type(config: Optional[TrainingLauncherConfig]) -> str:
    if config is None or isinstance(config, LocalTrainingLauncher):
        return "local"
    if isinstance(config, SkyVastTrainingLauncherConfig):
        return "sky_vast"
    return type(config).__name__


def launcher_config_json(config: Optional[TrainingLauncherConfig]) -> dict[str, Any]:
    if config is None:
        return {"launcher_type": "local"}
    if isinstance(config, (LocalTrainingLauncher, SkyVastTrainingLauncherConfig)):
        data = asdict(config)
    else:
        data = {"repr": repr(config)}
    data["launcher_type"] = launcher_type(config)
    return data


def initial_launcher_state(config: Optional[TrainingLauncherConfig]) -> dict[str, Any]:
    now = utc_iso()
    if isinstance(config, SkyVastTrainingLauncherConfig):
        return asdict(
            SkyVastTrainingRunState(
                started_at=now,
                last_seen_at=now,
            )
        )
    return asdict(LocalTrainingRunState(started_at=now, last_seen_at=now))


def active_lease(row: pd.Series, *, now: Optional[datetime] = None) -> bool:
    expires_at = parse_datetime(row.get("training_status__lease_expires_at"))
    return expires_at is not None and expires_at > (now or utc_now())


def attempts_reset_allowed(row: pd.Series, config: TrainingResumeConfig, *, now: Optional[datetime] = None) -> bool:
    if config.reset_attempts_after is None:
        return False
    reference = parse_datetime(row.get("training_status__finished_at")) or parse_datetime(
        row.get("training_status__heartbeat_at")
    )
    if reference is None:
        return False
    return (now or utc_now()) - reference >= parse_duration(config.reset_attempts_after)


def status_idx(training_status_id: str) -> IndexDF:
    return IndexDF(pd.DataFrame([{"training_status_id": training_status_id}]))


def status_id_for_attempt(run_key: str, attempt: int) -> str:
    return f"{run_key}__attempt_{attempt}"


def get_status_rows(dt: DataTable, run_key: str) -> pd.DataFrame:
    df = dt.get_data()
    if df.empty or "training_status__run_key" not in df.columns:
        return pd.DataFrame()
    return df[df["training_status__run_key"] == run_key].reset_index(drop=True)


def _status_sort_key(row: pd.Series) -> tuple[int, str]:
    attempt = int(row.get("training_status__attempt") or 0)
    ts = str(row.get("training_status__started_at") or row.get("training_status__heartbeat_at") or "")
    return attempt, ts


def get_status_row(dt: DataTable, run_key: str) -> Optional[pd.Series]:
    df = get_status_rows(dt, run_key)
    if df.empty:
        return None
    rows = sorted((row for _, row in df.iterrows()), key=_status_sort_key)
    return rows[-1]


def get_active_status_row(dt: DataTable, run_key: str) -> Optional[pd.Series]:
    df = get_status_rows(dt, run_key)
    for _, row in df.iterrows():
        if row.get("training_status__status") == "running" and active_lease(row):
            return row
    return None


def store_status_row(dt: DataTable, row: dict[str, Any]) -> None:
    dt.store_chunk(pd.DataFrame([row]))


def base_status_row(
    *,
    run_key: str,
    idx: IndexDF,
    model_other_primary_keys: list[str],
    frozen_dataset_id_col: str,
    frozen_dataset_id: Any,
    train_config_id_col: str,
    train_config_id: Any,
    model_id_col: str,
    model_id: str,
    models_dir: str,
    run_dir: str,
    launcher_config: Optional[TrainingLauncherConfig],
    attempt: int,
    status: str,
    manifest_path: Optional[str] = None,
    error: Optional[str] = None,
) -> dict[str, Any]:
    now = utc_now()
    row: dict[str, Any] = dict(
        training_status_id=status_id_for_attempt(run_key, attempt),
        training_status__run_key=run_key,
        training_status__launcher_type=launcher_type(launcher_config),
        training_status__launcher_config=launcher_config_json(launcher_config),
        training_status__launcher_state=initial_launcher_state(launcher_config),
        training_status__models_dir=models_dir,
        training_status__run_dir=run_dir,
        training_status__status=status,
        training_status__started_at=utc_iso(now),
        training_status__finished_at=None,
        training_status__attempt=attempt,
        training_status__manifest_path=manifest_path,
        training_status__error=error,
        training_status__owner_id=owner_id(),
        training_status__heartbeat_at=utc_iso(now),
        training_status__lease_expires_at=utc_iso(now + timedelta(minutes=10)),
    )
    row[frozen_dataset_id_col] = frozen_dataset_id
    row[train_config_id_col] = train_config_id
    row[model_id_col] = model_id
    if len(idx):
        idx_row = idx.iloc[0]
        for key in model_other_primary_keys:
            row[key] = idx_row[key]
    return row


class TrainingStatusManager:
    """Owns lifecycle writes to output__training_status around blocking training."""

    def __init__(self, *, dt: DataTable, row: dict[str, Any], heartbeat_interval_s: int = 60, lease_ttl_s: int = 600):
        self.dt = dt
        self.row = dict(row)
        self.heartbeat_interval_s = heartbeat_interval_s
        self.lease_ttl_s = lease_ttl_s
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        self.row["training_status__status"] = "running"
        self._touch_lease()
        self._store()
        self._thread = threading.Thread(target=self._heartbeat_loop, name="datapipe-training-status", daemon=True)
        self._thread.start()

    def _store(self) -> None:
        # Store directly because the surrounding DatatableBatchTransform is blocked
        # until training returns.
        store_status_row(self.dt, self.row)

    def _touch_lease(self) -> None:
        now = utc_now()
        self.row["training_status__heartbeat_at"] = utc_iso(now)
        self.row["training_status__lease_expires_at"] = utc_iso(now + timedelta(seconds=self.lease_ttl_s))

    def _heartbeat_loop(self) -> None:
        while not self._stop.wait(self.heartbeat_interval_s):
            self._touch_lease()
            self._store()

    def _finalize_status(
        self,
        *,
        status: str,
        manifest_path: Optional[str] = None,
        error: Optional[str] = None,
        run_dir: Optional[str] = None,
    ) -> None:
        self._stop_heartbeat()
        if run_dir is not None:
            self.row["training_status__run_dir"] = run_dir
        self.row.update(
            dict(
                training_status__status=status,
                training_status__finished_at=utc_iso(),
                training_status__manifest_path=manifest_path,
                training_status__error=error,
                training_status__owner_id=None,
                training_status__lease_expires_at=None,
            )
        )
        self._store()

    def mark_failed(self, *, error: str, manifest_path: Optional[str] = None) -> None:
        self._finalize_status(status="failed", manifest_path=manifest_path, error=error)

    def mark_completed(self, *, run_dir: str, manifest_path: Optional[str] = None) -> None:
        self._finalize_status(status="completed", run_dir=run_dir, manifest_path=manifest_path)

    def _stop_heartbeat(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5)
            self._thread = None

    def __enter__(self) -> "TrainingStatusManager":
        self.start()
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        self._stop_heartbeat()
