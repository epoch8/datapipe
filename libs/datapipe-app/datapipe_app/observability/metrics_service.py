from __future__ import annotations

import hashlib
import re
from collections import defaultdict
from datetime import datetime
from typing import Any, Optional

import pandas as pd
from datapipe.compute import Catalog
from datapipe.datatable import DataStore

from datapipe_app.observability.anomaly import (
    build_kpis,
    compute_deltas,
    detect_anomalies,
    pick_primary_value,
    primary_metric_for_task,
)
from datapipe_app.observability.schemas import (
    ClassMetricDetailResponse,
    ClassMetricRow,
    ClassMetricsResponse,
    MetricsRunRow,
    MetricsRunsResponse,
    MetricsSummaryResponse,
    MetricsTimeseriesResponse,
)

try:
    from datapipe_ml.observability.discovery import (
        discover_metrics_tables,
        infer_task_type,
        metric_columns,
        row_model_id,
        table_schema_columns,
    )
except ImportError:
    discover_metrics_tables = None  # type: ignore
    infer_task_type = lambda _n: None  # type: ignore
    metric_columns = lambda cols: [c for c in cols if c.startswith("calc__")]  # type: ignore
    row_model_id = lambda _r, _c: None  # type: ignore
    table_schema_columns = lambda _dt: []  # type: ignore


METRIC_ALIASES = {
    "mAP_0_5": "mAP50",
    "mAP_0_5_to_0_95": "mAP50_95",
}


def _normalize_metric_key(key: str) -> str:
    key = key.removeprefix("calc__")
    return METRIC_ALIASES.get(key, key)


def _safe_float(val: Any) -> Optional[float]:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _safe_int(val: Any) -> Optional[int]:
    f = _safe_float(val)
    return int(f) if f is not None else None


def _run_id(model_id: str, subset: str, idx: int) -> str:
    raw = f"{model_id}|{subset}|{idx}"
    return hashlib.md5(raw.encode()).hexdigest()[:12]


def _row_to_metrics(row: pd.Series, calc_cols: list[str]) -> dict[str, float | None]:
    metrics: dict[str, float | None] = {}
    for col in calc_cols:
        metrics[_normalize_metric_key(col)] = _safe_float(row.get(col))
    return metrics


def _is_class_table(name: str, columns: list[str]) -> bool:
    return "by_cls" in name or ("label" in columns and "metrics_by" in name)


def _load_catalog_rows(
    ds: Optional[DataStore],
    catalog: Optional[Catalog],
) -> tuple[list[MetricsRunRow], list[tuple[str, ClassMetricRow, str]], Optional[str]]:
    run_rows: list[MetricsRunRow] = []
    class_rows: list[tuple[str, ClassMetricRow, str]] = []
    task_type: Optional[str] = None

    if ds is None or catalog is None or discover_metrics_tables is None:
        return run_rows, class_rows, task_type

    for table_name, dt in discover_metrics_tables(catalog, ds):
        try:
            df = dt.get_data()
        except Exception:
            continue
        if df.empty:
            continue
        columns = table_schema_columns(dt)
        calc_cols = metric_columns(columns)
        if not calc_cols:
            continue
        tt = infer_task_type(table_name)
        if tt:
            task_type = tt
        is_class = _is_class_table(table_name, columns) or ("label" in columns and "metrics_on_subset" not in table_name)

        for idx, row in df.iterrows():
            model_id = row_model_id(row, columns) or ""
            subset = str(row.get("subset_id", "")) if not pd.isna(row.get("subset_id", None)) else ""
            label = row.get("label")
            has_label = label is not None and not (isinstance(label, float) and pd.isna(label)) and str(label).strip()

            if is_class and has_label:
                class_rows.append(
                    (
                        table_name,
                        ClassMetricRow(
                            label=str(label),
                            class_id=row.get("class_id"),
                            images_support=_safe_int(row.get("calc__images_support")),
                            support=_safe_int(row.get("calc__support")),
                            TP=_safe_int(row.get("calc__TP")),
                            FP=_safe_int(row.get("calc__FP")),
                            FN=_safe_int(row.get("calc__FN")),
                            precision=_safe_float(row.get("calc__precision")),
                            recall=_safe_float(row.get("calc__recall")),
                            f1_score=_safe_float(row.get("calc__f1_score")),
                            iou_mean=_safe_float(row.get("calc__iou_mean")),
                            mAP50=_safe_float(row.get("calc__mAP50")),
                            mAP50_95=_safe_float(row.get("calc__mAP50_95")),
                            pose_P=_safe_float(row.get("calc__pose_P")),
                            pose_R=_safe_float(row.get("calc__pose_R")),
                            pose_mAP50=_safe_float(row.get("calc__pose_mAP50")),
                            pose_mAP50_95=_safe_float(row.get("calc__pose_mAP50_95")),
                        ),
                        subset,
                    )
                )
            elif not has_label:
                metrics = _row_to_metrics(row, calc_cols)
                run_rows.append(
                    MetricsRunRow(
                        run_id=_run_id(model_id, subset, int(idx) if isinstance(idx, int) else 0),
                        pipeline_id="",
                        model_id=model_id,
                        task_type=tt,
                        subset=subset,
                        status="success",
                        metrics=metrics,
                    )
                )
    return run_rows, class_rows, task_type


def _load_observability_runs(store: Any, pipeline_id: str) -> list[MetricsRunRow]:
    rows: list[MetricsRunRow] = []
    if store is None:
        return rows
    try:
        metric_rows = store.list_metrics(pipeline_id)
    except Exception:
        return rows
    grouped: dict[tuple[str, str, str], dict[str, float | None]] = defaultdict(dict)
    meta: dict[tuple[str, str, str], dict[str, Any]] = {}
    for mr in metric_rows:
        key = (str(mr.model_id or ""), str(mr.subset_id or ""), mr.computed_at.isoformat() if mr.computed_at else "")
        grouped[key][mr.metric_name] = mr.metric_value
        meta[key] = {"task_type": mr.task_type, "computed_at": mr.computed_at}
    for i, (key, metrics) in enumerate(sorted(grouped.items(), key=lambda x: x[0][2], reverse=True)):
        model_id, subset, ts = key
        rows.append(
            MetricsRunRow(
                run_id=_run_id(model_id, subset, i) if not ts else ts[:12].replace("-", ""),
                pipeline_id=pipeline_id,
                model_id=model_id,
                subset=subset,
                started_at=ts or None,
                task_type=meta[key].get("task_type"),
                status="success",
                metrics=metrics,
            )
        )
    return rows


def _merge_runs(catalog_runs: list[MetricsRunRow], obs_runs: list[MetricsRunRow], pipeline_id: str) -> list[MetricsRunRow]:
    merged = obs_runs if obs_runs else catalog_runs
    for r in merged:
        r.pipeline_id = pipeline_id
    if obs_runs and catalog_runs:
        seen = {(r.model_id, r.subset) for r in obs_runs}
        for r in catalog_runs:
            if (r.model_id, r.subset) not in seen:
                r.pipeline_id = pipeline_id
                merged.append(r)
    return merged


def _filter_runs(
    runs: list[MetricsRunRow],
    *,
    subset: Optional[str] = None,
    model_id: Optional[str] = None,
    search: Optional[str] = None,
    from_dt: Optional[datetime] = None,
    to_dt: Optional[datetime] = None,
) -> list[MetricsRunRow]:
    result = runs
    if subset:
        result = [r for r in result if r.subset == subset]
    if model_id:
        result = [r for r in result if r.model_id == model_id]
    if search:
        q = search.lower()
        result = [r for r in result if q in r.run_id.lower() or q in r.model_id.lower() or q in r.subset.lower()]
    if from_dt or to_dt:
        filtered = []
        for r in result:
            if not r.started_at:
                filtered.append(r)
                continue
            try:
                dt = datetime.fromisoformat(r.started_at.replace("Z", "+00:00"))
            except ValueError:
                filtered.append(r)
                continue
            if from_dt and dt < from_dt:
                continue
            if to_dt and dt > to_dt:
                continue
            filtered.append(r)
        result = filtered
    return result


def _sort_runs(runs: list[MetricsRunRow], sort_by: Optional[str], sort_dir: str) -> list[MetricsRunRow]:
    if not sort_by:
        return runs
    reverse = sort_dir != "asc"

    def key_fn(r: MetricsRunRow) -> Any:
        if sort_by in r.metrics:
            return r.metrics.get(sort_by) or 0
        return getattr(r, sort_by, None) or ""

    return sorted(runs, key=key_fn, reverse=reverse)


CLASS_STRING_SORT_FIELDS = {"label", "class_id"}


def _parse_sort_specs(sort_by: Optional[str], sort_dir: str) -> list[tuple[str, str]]:
    if not sort_by:
        return []
    fields = [f.strip() for f in sort_by.split(",") if f.strip()]
    dirs = [d.strip().lower() for d in sort_dir.split(",") if d.strip()]
    if not dirs:
        dirs = ["desc"]
    while len(dirs) < len(fields):
        dirs.append(dirs[-1])
    return list(zip(fields, dirs[: len(fields)]))


def _class_sort_tuple_key(r: ClassMetricRow, specs: list[tuple[str, str]]) -> tuple[Any, ...]:
    parts: list[Any] = []
    for field, direction in specs:
        val = getattr(r, field, None)
        if val is None:
            parts.append((1, 0))
            continue
        if field in CLASS_STRING_SORT_FIELDS:
            text = (val or "").lower() if field == "label" else str(val)
            if direction == "asc":
                parts.append((0, text))
            else:
                parts.append((0, tuple(-ord(c) for c in text)))
        elif direction == "desc":
            parts.append((0, -float(val)))
        else:
            parts.append((0, val))
    return tuple(parts)


def _sort_class_rows(rows: list[ClassMetricRow], sort_by: Optional[str], sort_dir: str) -> list[ClassMetricRow]:
    specs = _parse_sort_specs(sort_by, sort_dir)
    if not specs:
        return rows
    return sorted(rows, key=lambda r: _class_sort_tuple_key(r, specs))


class MetricsService:
    def __init__(
        self,
        *,
        store: Any = None,
        ds: Optional[DataStore] = None,
        catalog: Optional[Catalog] = None,
    ) -> None:
        self.store = store
        self.ds = ds
        self.catalog = catalog

    def _all_runs(self, pipeline_id: str) -> tuple[list[MetricsRunRow], Optional[str]]:
        catalog_runs, _, task_type = _load_catalog_rows(self.ds, self.catalog)
        obs_runs = _load_observability_runs(self.store, pipeline_id)
        runs = _merge_runs(catalog_runs, obs_runs, pipeline_id)
        for i, run in enumerate(runs):
            if i + 1 < len(runs):
                compute_deltas(run, runs[i + 1])
        return runs, task_type

    def list_runs(
        self,
        pipeline_id: str,
        *,
        subset: Optional[str] = None,
        model_id: Optional[str] = None,
        search: Optional[str] = None,
        from_dt: Optional[datetime] = None,
        to_dt: Optional[datetime] = None,
        sort_by: Optional[str] = None,
        sort_dir: str = "desc",
        limit: int = 25,
        offset: int = 0,
    ) -> MetricsRunsResponse:
        runs, _ = self._all_runs(pipeline_id)
        runs = _filter_runs(runs, subset=subset, model_id=model_id, search=search, from_dt=from_dt, to_dt=to_dt)
        runs = _sort_runs(runs, sort_by or "started_at", sort_dir)
        total = len(runs)
        page = runs[offset : offset + limit]
        subsets = sorted({r.subset for r in runs if r.subset})
        models = sorted({r.model_id for r in runs if r.model_id})
        metric_keys = sorted({k for r in runs for k in r.metrics})
        return MetricsRunsResponse(
            rows=page,
            total=total,
            available_filters={"subsets": subsets, "models": models, "tags": [], "metrics": metric_keys},
        )

    def summary(
        self,
        pipeline_id: str,
        *,
        subset: Optional[str] = None,
        model_id: Optional[str] = None,
        primary_metric: Optional[str] = None,
    ) -> MetricsSummaryResponse:
        runs, task_type = self._all_runs(pipeline_id)
        runs = _filter_runs(runs, subset=subset, model_id=model_id)
        if not runs:
            return MetricsSummaryResponse(pipeline_id=pipeline_id, primary_metric=primary_metric or "f1_score", has_metrics=False)

        pm = primary_metric or primary_metric_for_task(task_type)
        best = max(runs, key=lambda r: pick_primary_value(r.metrics, task_type) or -1)
        latest = runs[0]
        previous = runs[1] if len(runs) > 1 else None
        return MetricsSummaryResponse(
            pipeline_id=pipeline_id,
            primary_metric=pm,
            has_metrics=True,
            latest_run=latest,
            best_run=best,
            previous_run=previous,
            kpis=build_kpis(runs, task_type, best),
            anomalies=detect_anomalies(runs, task_type),
        )

    def timeseries(
        self,
        pipeline_id: str,
        *,
        metrics: list[str],
        subset: Optional[list[str]] = None,
        group_by: str = "run",
    ) -> MetricsTimeseriesResponse:
        runs, _ = self._all_runs(pipeline_id)
        if subset:
            runs = [r for r in runs if r.subset in subset]
        series = []
        for metric in metrics:
            for sub in sorted({r.subset for r in runs}) or [""]:
                sub_runs = [r for r in runs if r.subset == sub] if sub else runs
                points = []
                for r in reversed(sub_runs):
                    val = r.metrics.get(metric)
                    if val is None:
                        continue
                    x = r.started_at[:10] if r.started_at else r.run_id
                    if group_by == "model":
                        x = r.model_id
                    points.append({"x": x, "y": val, "run_id": r.run_id})
                if points:
                    series.append(
                        {
                            "key": f"{metric}-{sub or 'all'}",
                            "label": f"{metric} ({sub or 'all'})",
                            "metric": metric,
                            "subset": sub or None,
                            "points": points,
                        }
                    )
        return MetricsTimeseriesResponse(series=series)

    def list_classes(
        self,
        pipeline_id: str,
        *,
        subset: Optional[str] = None,
        model_id: Optional[str] = None,
        label_search: Optional[str] = None,
        sort_by: Optional[str] = None,
        sort_dir: str = "desc",
        limit: int = 50,
        offset: int = 0,
    ) -> ClassMetricsResponse:
        _, class_rows, _ = _load_catalog_rows(self.ds, self.catalog)
        rows = [r for _, r, sub in class_rows if (not subset or sub == subset)]
        if label_search:
            q = label_search.lower()
            rows = [r for r in rows if q in r.label.lower()]
        rows = _sort_class_rows(rows, sort_by, sort_dir)
        total = len(rows)
        page = rows[offset : offset + limit]
        f1_vals = [r.f1_score for r in rows if r.f1_score is not None]
        macro_f1 = sum(f1_vals) / len(f1_vals) if f1_vals else None
        sorted_by_f1 = sorted(rows, key=lambda r: r.f1_score or 0, reverse=True)
        return ClassMetricsResponse(
            rows=page,
            total=total,
            summary={
                "total_classes": total,
                "macro_f1": macro_f1,
                "weighted_f1": macro_f1,
                "best_classes": sorted_by_f1[:3],
                "worst_classes": list(reversed(sorted_by_f1[-3:])),
            },
        )

    def class_detail(self, pipeline_id: str, label: str, *, subset: Optional[str] = None) -> ClassMetricDetailResponse:
        data = self.list_classes(pipeline_id, subset=subset, label_search=label, limit=1000)
        matches = [r for r in data.rows if r.label == label]
        latest = matches[0] if matches else (data.rows[0] if data.rows else ClassMetricRow(label=label))
        return ClassMetricDetailResponse(
            label=label,
            class_id=latest.class_id,
            latest=latest,
            previous=None,
            trends=[{"metric": "f1_score", "points": []}],
            error_breakdown={
                "false_negatives": latest.FN or 0,
                "false_positives": latest.FP or 0,
            },
        )
