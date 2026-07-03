from __future__ import annotations

from typing import Any, Optional

import pandas as pd
from datapipe.compute import Catalog
from datapipe.datatable import DataStore

from datapipe_ml.observability.discovery import discover_training_status_tables, table_schema_columns
from datapipe_ml.training.runs import active_lease, parse_datetime, utc_now


def _heartbeat_age_s(row: pd.Series) -> Optional[int]:
    heartbeat = parse_datetime(row.get("training_status__heartbeat_at"))
    if heartbeat is None:
        return None
    return max(0, int((utc_now() - heartbeat).total_seconds()))


def _status_row_to_dto(row: pd.Series, columns: list[str]) -> dict[str, Any]:
    status = str(row.get("training_status__status") or "")
    model_id = None
    for column in columns:
        if column.endswith("_model_id"):
            value = row.get(column)
            if value is not None and not pd.isna(value):
                model_id = str(value)
                break
    lease_expired = status == "running" and not active_lease(row)
    return {
        "run_key": str(row.get("training_status__run_key") or ""),
        "status": status,
        "attempt": int(row.get("training_status__attempt") or 0),
        "heartbeat_age_s": _heartbeat_age_s(row),
        "lease_expired": lease_expired,
        "error": None if pd.isna(row.get("training_status__error")) else row.get("training_status__error"),
        "model_id": model_id,
        "run_dir": None if pd.isna(row.get("training_status__run_dir")) else str(row.get("training_status__run_dir")),
        "started_at": row.get("training_status__started_at"),
        "finished_at": row.get("training_status__finished_at"),
        "launcher_type": row.get("training_status__launcher_type"),
    }


class TrainingStatusCollector:
    def collect_pipeline_status(
        self,
        *,
        pipeline_id: str,
        ds: DataStore | None,
        catalog: Catalog | None,
    ) -> list[dict[str, Any]]:
        if ds is None or catalog is None:
            return []
        rows: list[dict[str, Any]] = []
        for _table_name, dt in discover_training_status_tables(catalog, ds):
            df = dt.get_data()
            if df.empty:
                continue
            columns = table_schema_columns(dt)
            for _, row in df.iterrows():
                dto = _status_row_to_dto(row, columns)
                if dto["run_key"]:
                    rows.append(dto)
        rows.sort(key=lambda item: (item.get("started_at") or "", item.get("attempt") or 0))
        return rows
