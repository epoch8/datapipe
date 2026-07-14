from __future__ import annotations

from typing import Any

import pandas as pd
from datapipe.compute import Catalog
from datapipe.datatable import DataStore

from datapipe_app.ops_spec_resolve import column_by_kind, column_source_by_link
from datapipe_app_ml_ops.ops_specs_service import OpsSpecsService
from datapipe_app_ml_ops.spec_registry import OpsSpecRegistry
from datapipe_app_ml_ops.ops_specs import DatapipeOpsSpec


def best_model_ids_from_specs(registry: OpsSpecRegistry, ds: DataStore, catalog: Catalog) -> set[str]:
    best: set[str] = set()
    for spec in registry.list():
        model = spec.model
        if model is None or not model.is_best_table or not model.is_best_column:
            continue
        dt = ds.get_table(model.is_best_table)
        try:
            df = dt.get_data()
        except Exception:
            continue
        for _, row in df.iterrows():
            if not row.get(model.is_best_column):
                continue
            model_id = row.get(model.id_column)
            if model_id is not None and not pd.isna(model_id):
                best.add(str(model_id))
    return best


def _task_type(spec: DatapipeOpsSpec) -> str:
    if spec.tags:
        return spec.tags[0]
    return spec.id


def list_training_runs_from_specs(
    registry: OpsSpecRegistry,
    ds: DataStore,
    catalog: Catalog,
    pipeline_id: str,
) -> list[dict[str, Any]]:
    if not registry.list():
        return []

    service = OpsSpecsService(registry, ds=ds, catalog=catalog)
    best_models = best_model_ids_from_specs(registry, ds, catalog)
    rows: list[dict[str, Any]] = []

    for spec in registry.list():
        training = spec.training
        if training is None:
            continue
        payload = service.training_rows(spec.id, limit=500)
        run_col = column_source_by_link(training.columns, "training_run")
        model_col = column_source_by_link(training.columns, "model")
        dataset_col = column_source_by_link(training.columns, "frozen_dataset")
        status_col = column_by_kind(training.columns, "status")
        started_col = next((column.source for column in training.columns if column.kind == "datetime"), None)
        finished_col = started_col.replace("started_at", "finished_at") if started_col and "started_at" in started_col else None
        launcher_col = next((column.source for column in training.columns if "launcher_type" in column.source), None)

        for row in payload["rows"]:
            run_key = str(row.get(run_col) or "") if run_col else ""
            model_id = row.get(model_col) if model_col else None
            model_text = str(model_id) if model_id is not None and not pd.isna(model_id) else None
            started_at = row.get(started_col) if started_col else None
            finished_at = row.get(finished_col) if finished_col else None
            duration_s = row.get("duration_seconds")
            rows.append(
                {
                    "run_key": run_key,
                    "status": row.get(status_col.source) if status_col else None,
                    "attempt": row.get(next((c.source for c in training.columns if "attempt" in c.source), ""), 0),
                    "model_id": model_text,
                    "dataset_id": str(row.get(dataset_col)) if dataset_col and row.get(dataset_col) is not None and not pd.isna(row.get(dataset_col)) else None,
                    "started_at": started_at,
                    "finished_at": finished_at,
                    "duration_s": int(duration_s) if duration_s is not None and not pd.isna(duration_s) else None,
                    "launcher_type": row.get(launcher_col) if launcher_col else None,
                    "is_best": model_text in best_models if model_text else False,
                    "task_type": _task_type(spec),
                    "spec_id": spec.id,
                    "pipeline_id": pipeline_id,
                }
            )

    rows.sort(key=lambda item: (item.get("started_at") or "", item.get("run_key") or ""))
    return rows


def build_run_detail_from_specs(
    registry: OpsSpecRegistry,
    ds: DataStore,
    catalog: Catalog,
    *,
    run_key: str,
    pipeline_id: str,
) -> dict[str, Any] | None:
    service = OpsSpecsService(registry, ds=ds, catalog=catalog)
    best_models = best_model_ids_from_specs(registry, ds, catalog)

    for spec in registry.list():
        training = spec.training
        if training is None:
            continue
        run_col = column_source_by_link(training.columns, "training_run")
        model_col = column_source_by_link(training.columns, "model")
        status_col = column_by_kind(training.columns, "status")
        if not run_col:
            continue
        payload = service.training_rows(spec.id, limit=500, filters={run_col: run_key})
        if not payload["rows"]:
            continue
        row = payload["rows"][0]
        model_id = row.get(model_col) if model_col else None
        model_text = str(model_id) if model_id is not None and not pd.isna(model_id) else None
        artifacts = {
            key: row.get(column)
            for key, column in training.artifact_columns.items()
        }
        return {
            "run_key": run_key,
            "pipeline_id": pipeline_id,
            "status": row.get(status_col.source) if status_col else None,
            "model_id": model_text,
            "params": {
                "launcher_type": next((row.get(column.source) for column in training.columns if "launcher_type" in column.source), None),
            },
            "artifacts": artifacts,
            "is_best": model_text in best_models if model_text else False,
            "task_type": _task_type(spec),
            "spec_id": spec.id,
        }
    return None
