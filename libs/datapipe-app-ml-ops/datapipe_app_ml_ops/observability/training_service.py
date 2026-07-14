from __future__ import annotations

import math
from typing import Any, Optional

from datapipe_app_ml_ops.ops_spec_metrics import latest_eval_metric_from_specs
from datapipe_app_ml_ops.observability.schemas import TrainingCompareResponse, TrainingRunRow, TrainingRunsResponse
from datapipe_app_ml_ops.observability.spec_training import list_training_runs_from_specs


def _is_nan(val: Any) -> bool:
    if val is None:
        return True
    if isinstance(val, float) and math.isnan(val):
        return True
    try:
        import pandas as pd

        if pd.isna(val):
            return True
    except Exception:
        pass
    return str(val).lower() == "nan"


def _clean_str(val: Any) -> Optional[str]:
    if _is_nan(val):
        return None
    return str(val)


def _clean_optional_str(val: Any) -> Optional[str]:
    if _is_nan(val):
        return None
    text = str(val).strip()
    return text or None


def _clean_float(val: Any) -> Optional[float]:
    if _is_nan(val):
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _launcher_framework(launcher_type: Optional[str]) -> Optional[str]:
    return str(launcher_type) if launcher_type else None


class TrainingService:
    def __init__(
        self,
        *,
        store: Any = None,
        ds: Any = None,
        catalog: Any = None,
        ops_specs: Any = None,
    ) -> None:
        self.store = store
        self.ds = ds
        self.catalog = catalog
        self.ops_specs = ops_specs

    def _raw_runs(self, pipeline_id: str) -> list[dict[str, Any]]:
        if self.ops_specs is None or self.ds is None or self.catalog is None or not self.ops_specs.list():
            return []
        return list_training_runs_from_specs(self.ops_specs, self.ds, self.catalog, pipeline_id)

    def list_runs(
        self,
        pipeline_id: str,
        *,
        task_type: Optional[list[str]] = None,
        framework: Optional[list[str]] = None,
        status: Optional[list[str]] = None,
        tags: Optional[list[str]] = None,
        search: Optional[str] = None,
        sort_by: Optional[str] = None,
        sort_dir: str = "desc",
        limit: int = 25,
        offset: int = 0,
    ) -> TrainingRunsResponse:
        raw = self._raw_runs(pipeline_id)

        rows: list[TrainingRunRow] = []
        for r in raw:
            eval_m = r.get("eval_metric") or {}
            if not eval_m and self.ops_specs is not None and self.ds is not None and self.catalog is not None:
                model_id = r.get("model_id")
                eval_m = latest_eval_metric_from_specs(
                    self.ops_specs,
                    self.ds,
                    self.catalog,
                    model_id=str(model_id) if model_id is not None else None,
                ) or {}
            best_name = eval_m.get("metric_name")
            best_val = eval_m.get("metric_value")
            tt = r.get("task_type")
            fw = _launcher_framework(_clean_optional_str(r.get("launcher_type")))
            rows.append(
                TrainingRunRow(
                    run_key=_clean_str(r.get("run_key")) or "",
                    run_id=_clean_str(r.get("run_key")) or "",
                    model_id=_clean_optional_str(r.get("model_id")),
                    task_type=tt,
                    framework=fw,
                    dataset=_clean_optional_str(r.get("dataset_id")),
                    started_at=_clean_optional_str(r.get("started_at")),
                    finished_at=_clean_optional_str(r.get("finished_at")),
                    duration_s=int(r["duration_s"]) if not _is_nan(r.get("duration_s")) and r.get("duration_s") is not None else None,
                    status=_clean_str(r.get("status")) or "unknown",
                    attempt=int(r["attempt"]) if not _is_nan(r.get("attempt")) and r.get("attempt") is not None else None,
                    tags=r.get("tags") or [],
                    best_metric_name=_clean_optional_str(best_name),
                    best_metric_value=_clean_float(best_val),
                    params={"launcher_type": _clean_optional_str(r.get("launcher_type")), "attempt": r.get("attempt")},
                    artifacts={
                        "run_dir": _clean_optional_str(r.get("run_dir")),
                    },
                    is_best=bool(r.get("is_best")),
                )
            )

        if task_type:
            rows = [r for r in rows if r.task_type in task_type]
        if framework:
            rows = [r for r in rows if r.framework in framework]
        if status:
            rows = [r for r in rows if r.status in status]
        if tags:
            rows = [r for r in rows if any(t in r.tags for t in tags)]
        if search:
            q = search.lower()
            rows = [r for r in rows if q in r.run_key.lower() or q in (r.model_id or "").lower()]

        if sort_by:
            reverse = sort_dir != "asc"

            def _sort_key(row: TrainingRunRow) -> str:
                value = row.model_dump().get(sort_by)
                return "" if value is None else str(value)

            rows = sorted(rows, key=_sort_key, reverse=reverse)

        total = len(rows)
        page = rows[offset : offset + limit]
        filters = {
            "task_types": sorted({r.task_type for r in rows if r.task_type}),
            "frameworks": sorted({r.framework for r in rows if r.framework}),
            "datasets": sorted({r.dataset for r in rows if r.dataset}),
            "statuses": sorted({r.status for r in rows}),
            "tags": sorted({t for r in rows for t in r.tags}),
        }
        return TrainingRunsResponse(rows=page, total=total, filters=filters)

    def compare(
        self,
        run_keys: list[str],
        *,
        metrics: Optional[list[str]] = None,
        pipeline_id: Optional[str] = None,
    ) -> TrainingCompareResponse:
        if len(run_keys) < 1 or len(run_keys) > 4:
            raise ValueError("Provide 1-4 run_keys")

        runs_resp = self.list_runs(pipeline_id or "", limit=100) if pipeline_id else TrainingRunsResponse(rows=[], total=0, filters={})
        matched_runs = [r for r in runs_resp.rows if r.run_key in run_keys]

        return TrainingCompareResponse(
            runs=matched_runs,
            available_metrics=[],
            charts=[],
            run_keys=run_keys,
            charts_legacy=[],
        )
