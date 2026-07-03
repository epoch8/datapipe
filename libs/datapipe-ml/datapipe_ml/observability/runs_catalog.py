from __future__ import annotations

from typing import Any, Optional

import pandas as pd
from datapipe.compute import Catalog
from datapipe.datatable import DataStore

from datapipe_ml.observability.discovery import (
    discover_training_status_tables,
    infer_task_type,
    model_id_column,
    table_schema_columns,
)
from datapipe_ml.observability.training import TrainingStatusCollector
from datapipe_ml.training.runs import parse_datetime


def _duration_seconds(started_at: Any, finished_at: Any) -> Optional[int]:
    start = parse_datetime(started_at)
    finish = parse_datetime(finished_at)
    if start is None or finish is None:
        return None
    return max(0, int((finish - start).total_seconds()))


def _find_best_model_rows(catalog: Catalog, ds: DataStore) -> dict[str, str]:
    best_by_model: dict[str, str] = {}
    for name in catalog.catalog:
        if "best_" not in name or not name.endswith("_model"):
            continue
        dt = ds.get_table(name)
        df = dt.get_data()
        if df.empty:
            continue
        columns = table_schema_columns(dt)
        model_col = model_id_column(columns)
        if model_col is None:
            continue
        for _, row in df.iterrows():
            model_id = row.get(model_col)
            if model_id is not None and not pd.isna(model_id):
                best_by_model[str(model_id)] = name
    return best_by_model


def _latest_eval_metric(store: Any, pipeline_id: str, model_id: Optional[str]) -> Optional[dict[str, Any]]:
    if store is None or model_id is None:
        return None
    rows = store.list_metrics(pipeline_id, model_id=model_id)
    if not rows:
        return None
    f1_rows = [row for row in rows if row.metric_name == "f1_score"]
    target = f1_rows[-1] if f1_rows else rows[-1]
    return {
        "metric_name": target.metric_name,
        "metric_value": target.metric_value,
        "computed_at": target.computed_at.isoformat() if target.computed_at else None,
    }


def _latest_epoch_metric(store: Any, training_run_key: str) -> Optional[dict[str, Any]]:
    if store is None:
        return None
    rows = store.list_training_epoch_metrics(training_run_key)
    if not rows:
        return None
    latest_epoch = max(row.epoch for row in rows)
    epoch_rows = [row for row in rows if row.epoch == latest_epoch]
    total_epochs = next((row.total_epochs for row in epoch_rows if row.total_epochs), None)
    metrics = {row.metric_name: row.metric_value for row in epoch_rows}
    return {
        "epoch": latest_epoch,
        "total_epochs": total_epochs,
        "metrics": metrics,
    }


class TrainingRunCatalog:
    def list_runs(
        self,
        *,
        pipeline_id: str,
        ds: DataStore | None,
        catalog: Catalog | None,
        store: Any = None,
    ) -> list[dict[str, Any]]:
        rows = TrainingStatusCollector().collect_pipeline_status(
            pipeline_id=pipeline_id,
            ds=ds,
            catalog=catalog,
        )
        best_models = _find_best_model_rows(catalog, ds) if catalog is not None and ds is not None else {}
        result: list[dict[str, Any]] = []
        for row in rows:
            run_key = row["run_key"]
            latest_epoch = _latest_epoch_metric(store, run_key)
            eval_metric = _latest_eval_metric(store, pipeline_id, row.get("model_id"))
            model_id = row.get("model_id")
            result.append(
                {
                    **row,
                    "duration_s": _duration_seconds(row.get("started_at"), row.get("finished_at")),
                    "latest_epoch": latest_epoch.get("epoch") if latest_epoch else None,
                    "total_epochs": latest_epoch.get("total_epochs") if latest_epoch else None,
                    "latest_training_metric": latest_epoch.get("metrics") if latest_epoch else None,
                    "eval_metric": eval_metric,
                    "is_best": model_id in best_models if model_id else False,
                    "best_model_table": best_models.get(model_id or "", None),
                }
            )
        return result

    def build_run_detail(
        self,
        *,
        run_key: str,
        pipeline_id: str,
        ds: DataStore | None,
        catalog: Catalog | None,
        store: Any = None,
    ) -> Optional[dict[str, Any]]:
        if ds is None or catalog is None:
            return None
        status_row: Optional[dict[str, Any]] = None
        status_table_name = ""
        for table_name, dt in discover_training_status_tables(catalog, ds):
            status_table_name = table_name
            df = dt.get_data()
            if df.empty:
                continue
            matched = df[df["training_status__run_key"] == run_key]
            if matched.empty:
                continue
            columns = table_schema_columns(dt)
            from datapipe_ml.observability.training import _status_row_to_dto

            status_row = _status_row_to_dto(matched.iloc[-1], columns)
            launcher_config = matched.iloc[-1].get("training_status__launcher_config")
            if launcher_config is not None and not isinstance(launcher_config, dict):
                try:
                    import json

                    launcher_config = json.loads(launcher_config)
                except Exception:
                    launcher_config = None
            status_row["launcher_config"] = launcher_config
            break
        if status_row is None:
            return None

        latest_epoch = _latest_epoch_metric(store, run_key)
        eval_metric = _latest_eval_metric(store, pipeline_id, status_row.get("model_id"))
        best_models = _find_best_model_rows(catalog, ds)
        model_id = status_row.get("model_id")
        artifacts = {
            "run_dir": status_row.get("run_dir"),
            "manifest_path": None,
        }
        for _table_name, dt in discover_training_status_tables(catalog, ds):
            df = dt.get_data()
            matched = df[df["training_status__run_key"] == run_key]
            if matched.empty:
                continue
            manifest = matched.iloc[-1].get("training_status__manifest_path")
            if manifest is not None and not pd.isna(manifest):
                artifacts["manifest_path"] = str(manifest)
            break

        params: dict[str, Any] = {}
        if status_row.get("launcher_config"):
            params["launcher"] = status_row["launcher_config"]
        params["launcher_type"] = status_row.get("launcher_type")
        params["attempt"] = status_row.get("attempt")

        return {
            "run_key": run_key,
            "pipeline_id": pipeline_id,
            "status": status_row.get("status"),
            "model_id": model_id,
            "params": params,
            "artifacts": artifacts,
            "latest_epoch": latest_epoch,
            "eval_metric": eval_metric,
            "is_best": model_id in best_models if model_id else False,
            "task_type": infer_task_type(status_table_name),
        }
