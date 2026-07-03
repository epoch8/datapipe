from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Optional

import pandas as pd
from datapipe.compute import Catalog
from datapipe.datatable import DataStore

from datapipe_ml.observability.discovery import (
    discover_metrics_tables,
    infer_task_type,
    metric_columns,
    row_model_id,
    table_schema_columns,
)

logger = logging.getLogger(__name__)


def _strip_calc_prefix(name: str) -> str:
    return name.removeprefix("calc__")


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class MLMetricsPublisher:
    def publish_metrics(
        self,
        store: Any,
        *,
        pipeline_id: str,
        ds: DataStore,
        catalog: Catalog,
    ) -> None:
        for table_name, dt in discover_metrics_tables(catalog, ds):
            if "metrics_on_subset" not in table_name and table_name.endswith("_metrics_on_image"):
                continue
            try:
                df = dt.get_data()
            except Exception:
                logger.exception("Failed to read metrics table %s", table_name)
                continue
            if df.empty:
                continue
            columns = table_schema_columns(dt)
            calc_columns = metric_columns(columns)
            if not calc_columns:
                continue
            task_type = infer_task_type(table_name)
            computed_at = _utc_now()
            for _, row in df.iterrows():
                model_id = row_model_id(row, columns)
                subset_id = row.get("subset_id")
                subset_value = None if pd.isna(subset_id) else str(subset_id)
                for column in calc_columns:
                    value = row.get(column)
                    if value is None or (isinstance(value, float) and pd.isna(value)):
                        continue
                    try:
                        metric_value = float(value)
                    except (TypeError, ValueError):
                        continue
                    store.upsert_pipeline_metric(
                        {
                            "pipeline_id": pipeline_id,
                            "model_id": model_id,
                            "subset_id": subset_value,
                            "metric_name": _strip_calc_prefix(column),
                            "metric_value": metric_value,
                            "computed_at": computed_at,
                            "source_table": table_name,
                            "task_type": task_type,
                        }
                    )
