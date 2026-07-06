from __future__ import annotations

import math
from typing import Any, Optional

from datapipe_app.observability.queries import build_training_curves
from datapipe_app.observability.schemas import TrainingCompareResponse, TrainingRunRow, TrainingRunsResponse

_training_run_catalog_cls: type[Any] | None
try:
    from datapipe_ml.observability.runs_catalog import TrainingRunCatalog as _training_run_catalog_cls
except ImportError:
    _training_run_catalog_cls = None

RUN_COLORS = ["purple", "blue", "orange", "green"]

METRIC_GROUPS = {
    "train_box_loss": "loss",
    "train_cls_loss": "loss",
    "train_dfl_loss": "loss",
    "val_box_loss": "loss",
    "val_cls_loss": "loss",
    "val_dfl_loss": "loss",
    "loss": "loss",
    "val_loss": "loss",
    "metrics_mAP_0_5": "metrics",
    "metrics_mAP_0_5_to_0_95": "metrics",
    "metrics_precision": "metrics",
    "metrics_recall": "metrics",
    "lr_pg0": "learning_rate",
    "lr": "learning_rate",
    "learning_rate": "learning_rate",
}


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


def _infer_framework(model_id: Optional[str], launcher_type: Optional[str]) -> Optional[str]:
    if launcher_type:
        return str(launcher_type)
    if model_id and "yolo" in model_id.lower():
        return "YOLOv8"
    return None


class TrainingService:
    def __init__(self, *, store: Any = None, ds: Any = None, catalog: Any = None) -> None:
        self.store = store
        self.ds = ds
        self.catalog = catalog
        self._catalog = _training_run_catalog_cls() if _training_run_catalog_cls is not None else None

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
        raw: list[dict[str, Any]] = []
        if self._catalog:
            raw = self._catalog.list_runs(
                pipeline_id=pipeline_id,
                ds=self.ds,
                catalog=self.catalog,
                store=self.store,
            )

        rows: list[TrainingRunRow] = []
        for r in raw:
            eval_m = r.get("eval_metric") or {}
            latest = r.get("latest_training_metric") or {}
            best_name = eval_m.get("metric_name")
            best_val = eval_m.get("metric_value")
            if best_val is None and latest:
                for k, v in latest.items():
                    if "map" in k.lower() or "f1" in k.lower():
                        best_name, best_val = k, v
                        break
            tt = r.get("task_type") or "detection"
            fw = _infer_framework(_clean_optional_str(r.get("model_id")), _clean_optional_str(r.get("launcher_type")))
            rows.append(
                TrainingRunRow(
                    run_key=_clean_str(r.get("run_key")) or "",
                    run_id=_clean_str(r.get("run_key")) or "",
                    model_id=_clean_optional_str(r.get("model_id")),
                    task_type=tt,
                    framework=fw,
                    dataset=_clean_optional_str(r.get("dataset")) or "default",
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
                        "best_model_table": _clean_optional_str(r.get("best_model_table")),
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

        all_metric_names: set[str] = set()
        charts_by_metric: dict[str, dict[str, Any]] = {}
        legacy_charts: list[dict[str, Any]] = []

        for ri, key in enumerate(run_keys):
            curve_charts = build_training_curves(self.store, key) if self.store else []
            legacy_charts.extend(curve_charts)
            for chart in curve_charts:
                for series in chart.get("series", []):
                    metric_name = series.get("label") or series.get("key", "")
                    all_metric_names.add(metric_name)
                    if metrics and metric_name not in metrics and not any(m in metric_name for m in metrics):
                        continue
                    entry = charts_by_metric.setdefault(
                        metric_name,
                        {
                            "metric": metric_name,
                            "title": chart.get("title", metric_name),
                            "x_label": "epoch",
                            "series": [],
                        },
                    )
                    entry["series"].append(
                        {
                            "run_key": key,
                            "label": key,
                            "color_key": str(ri),
                            "points": [{"x": p["x"], "y": p["y"]} for p in series.get("data", [])],
                        }
                    )

        available = [
            {
                "key": name,
                "label": name,
                "group": METRIC_GROUPS.get(name, "metrics"),
                "higher_is_better": "loss" not in name.lower(),
            }
            for name in sorted(all_metric_names)
        ]

        runs_resp = self.list_runs(pipeline_id or "", limit=100) if pipeline_id else TrainingRunsResponse(rows=[], total=0, filters={})
        matched_runs = [r for r in runs_resp.rows if r.run_key in run_keys]

        return TrainingCompareResponse(
            runs=matched_runs,
            available_metrics=available,
            charts=list(charts_by_metric.values()),
            run_keys=run_keys,
            charts_legacy=legacy_charts,
        )
