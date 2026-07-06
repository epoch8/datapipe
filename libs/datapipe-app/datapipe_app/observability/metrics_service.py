from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional
from urllib.parse import quote

import pandas as pd
from datapipe.compute import Catalog
from datapipe.datatable import DataStore

from datapipe_app.observability.db import PipelineMetricsCandidateRow, utc_now
from datapipe_app.observability.anomaly import (
    build_kpis,
    compute_deltas,
    detect_anomalies,
    pick_primary_value,
    primary_metric_for_task,
)
from datapipe_app.observability.metrics_schema import build_metric_schema
from datapipe_app.observability.schemas import (
    ClassMetricDetailResponse,
    ClassMetricRow,
    ClassMetricsResponse,
    EntitySourceRecord,
    FrozenDatasetCoverage,
    FrozenDatasetDetailResponse,
    FrozenDatasetLinkedModelRow,
    FrozenDatasetRow,
    FrozenDatasetsResponse,
    MetricsCandidateCreate,
    MetricsCandidateRow,
    MetricsCandidatesResponse,
    MetricsModelDetailKpi,
    MetricsModelDetailRelated,
    MetricsModelDetailResponse,
    MetricsModelRow,
    MetricsRunRow,
    MetricsRunsResponse,
    MetricsSummaryResponse,
    MetricsTableSchema,
    MetricsTimeseriesResponse,
    SplitCounts,
)

try:
    from datapipe_ml.observability.discovery import (
        discover_metrics_tables,
        discover_training_status_tables,
        frozen_dataset_id_column,
        frozen_dataset_metadata_columns,
        infer_task_type,
        is_frozen_dataset_table,
        is_metrics_table,
        is_model_frozen_dataset_link_table,
        is_training_status_table,
        metric_columns,
        model_id_column,
        row_model_id,
        table_schema_columns,
    )
except ImportError:
    discover_metrics_tables = None  # type: ignore
    discover_training_status_tables = lambda _c, _d: []  # type: ignore
    infer_task_type = lambda _n: None  # type: ignore
    metric_columns = lambda cols: [c for c in cols if c.startswith("calc__")]  # type: ignore
    row_model_id = lambda _r, _c: None  # type: ignore
    table_schema_columns = lambda _dt: []  # type: ignore
    model_id_column = lambda _c: None  # type: ignore
    is_frozen_dataset_table = lambda _n: False  # type: ignore
    is_model_frozen_dataset_link_table = lambda _n: False  # type: ignore
    is_metrics_table = lambda _n, _dt: False  # type: ignore
    is_training_status_table = lambda _n: False  # type: ignore
    frozen_dataset_id_column = lambda _c: None  # type: ignore
    frozen_dataset_metadata_columns = lambda _c: {}  # type: ignore


METRIC_ALIASES = {
    "mAP_0_5": "mAP50",
    "mAP_0_5_to_0_95": "mAP50_95",
}

_MODEL_COUNT_KEYS = frozenset(
    {
        "images_support",
        "support",
        "TP",
        "FP",
        "FN",
        "TP_extra_bbox",
        "FP_extra_bbox",
        "FN_extra_bbox",
        "images",
        "objects",
        "detections",
        "false_positives",
        "false_negatives",
    }
)


def _candidate_row_from_db(row: Any) -> MetricsCandidateRow:
    return MetricsCandidateRow(
        id=row.id,
        pipeline_id=row.pipeline_id,
        model_id=row.model_id,
        model_source=row.model_source,
        artifact_uri=row.artifact_uri,
        dataset_id=row.dataset_id,
        subset=row.subset,
        task_type=row.task_type,
        metrics_state=row.metrics_state,
    )


def _load_candidates(store: Any, pipeline_id: str) -> list[MetricsCandidateRow]:
    if store is None:
        return []
    return [_candidate_row_from_db(row) for row in store.list_metrics_candidates(pipeline_id)]


@dataclass
class EntitySourceIndex:
    model_records_by_id: dict[str, EntitySourceRecord] = field(default_factory=dict)
    dataset_records_by_id: dict[str, EntitySourceRecord] = field(default_factory=dict)
    model_to_dataset: dict[str, str] = field(default_factory=dict)
    dataset_to_models: dict[str, list[str]] = field(default_factory=dict)
    dataset_model_link_records: dict[tuple[str, str], EntitySourceRecord] = field(default_factory=dict)
    _model_priority: dict[str, int] = field(default_factory=dict)
    _dataset_priority: dict[str, int] = field(default_factory=dict)


def _jsonable(val: Any) -> Any:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    if hasattr(val, "isoformat"):
        return val.isoformat()
    if isinstance(val, (str, int, float, bool)):
        return val
    return str(val)


def _row_to_record(row: pd.Series, columns: list[str]) -> dict[str, Any]:
    return {col: _jsonable(row.get(col)) for col in columns}


def _row_pk(row: pd.Series, pk_cols: list[str]) -> dict[str, Any] | None:
    if not pk_cols:
        return None
    pk = {col: _jsonable(row.get(col)) for col in pk_cols}
    if all(v is None for v in pk.values()):
        return None
    return pk


def _table_row_url(
    pipeline_id: str,
    table_name: str | None,
    focus_col: str | None,
    focus_value: str | None,
) -> str | None:
    if not table_name or not focus_col or focus_value is None:
        return None
    qs = f"focus_col={quote(focus_col)}&focus_value={quote(str(focus_value))}"
    return f"/pipelines/{quote(pipeline_id)}/tables/{quote(table_name)}?{qs}"


def _maybe_set_model_record(index: EntitySourceIndex, model_id: str, record: EntitySourceRecord, priority: int) -> None:
    if model_id not in index.model_records_by_id or index._model_priority.get(model_id, 99) > priority:
        index.model_records_by_id[model_id] = record
        index._model_priority[model_id] = priority


def _maybe_set_dataset_record(index: EntitySourceIndex, dataset_id: str, record: EntitySourceRecord, priority: int) -> None:
    if dataset_id not in index.dataset_records_by_id or index._dataset_priority.get(dataset_id, 99) > priority:
        index.dataset_records_by_id[dataset_id] = record
        index._dataset_priority[dataset_id] = priority


def _is_likely_model_table(name: str, columns: list[str]) -> bool:
    if is_training_status_table(name):
        return True
    model_col = model_id_column(columns)
    if not model_col:
        return False
    lowered = name.lower()
    return "model" in lowered and not is_frozen_dataset_table(name) and not is_model_frozen_dataset_link_table(name)


def _load_entity_source_records(
    ds: Optional[DataStore],
    catalog: Optional[Catalog],
    *,
    candidates: Optional[list[MetricsCandidateRow]] = None,
) -> EntitySourceIndex:
    index = EntitySourceIndex()
    if ds is None or catalog is None:
        return index

    for name in catalog.catalog:
        dt = ds.get_table(name)
        try:
            df = dt.get_data()
        except Exception:
            continue
        if df.empty:
            continue
        columns = table_schema_columns(dt)
        pk_cols = list(dt.primary_keys)

        if is_frozen_dataset_table(name):
            id_col = frozen_dataset_id_column(columns)
            if not id_col:
                continue
            for _, row in df.iterrows():
                dataset_id = row.get(id_col)
                if dataset_id is None or (isinstance(dataset_id, float) and pd.isna(dataset_id)):
                    continue
                did = str(dataset_id)
                _maybe_set_dataset_record(
                    index,
                    did,
                    EntitySourceRecord(
                        table_name=name,
                        pk=_row_pk(row, pk_cols) or {id_col: did},
                        record=_row_to_record(row, columns),
                    ),
                    priority=1,
                )

        if is_model_frozen_dataset_link_table(name):
            model_col = model_id_column(columns)
            dataset_col = frozen_dataset_id_column(columns)
            if not model_col or not dataset_col:
                continue
            for _, row in df.iterrows():
                model_value = row.get(model_col)
                dataset_value = row.get(dataset_col)
                if model_value is None or dataset_value is None:
                    continue
                if isinstance(model_value, float) and pd.isna(model_value):
                    continue
                if isinstance(dataset_value, float) and pd.isna(dataset_value):
                    continue
                mid, did = str(model_value), str(dataset_value)
                index.model_to_dataset[mid] = did
                index.dataset_to_models.setdefault(did, [])
                if mid not in index.dataset_to_models[did]:
                    index.dataset_to_models[did].append(mid)
                record = EntitySourceRecord(
                    table_name=name,
                    pk=_row_pk(row, pk_cols),
                    record=_row_to_record(row, columns),
                )
                index.dataset_model_link_records[(did, mid)] = record
                _maybe_set_model_record(index, mid, record, priority=2)
                _maybe_set_dataset_record(index, did, record, priority=3)

        if _is_likely_model_table(name, columns):
            model_col = model_id_column(columns)
            if not model_col:
                continue
            priority = 1 if is_training_status_table(name) else 1
            if is_training_status_table(name):
                priority = 3
            elif "model" in name.lower():
                priority = 1
            for _, row in df.iterrows():
                model_value = row.get(model_col)
                if model_value is None or (isinstance(model_value, float) and pd.isna(model_value)):
                    continue
                mid = str(model_value)
                _maybe_set_model_record(
                    index,
                    mid,
                    EntitySourceRecord(
                        table_name=name,
                        pk=_row_pk(row, pk_cols) or {model_col: mid},
                        record=_row_to_record(row, columns),
                    ),
                    priority=priority,
                )

        if discover_metrics_tables is not None and is_metrics_table(name, dt):
            model_col = model_id_column(columns)
            if not model_col:
                continue
            for _, row in df.iterrows():
                model_value = row.get(model_col)
                if model_value is None or (isinstance(model_value, float) and pd.isna(model_value)):
                    continue
                mid = str(model_value)
                _maybe_set_model_record(
                    index,
                    mid,
                    EntitySourceRecord(
                        table_name=name,
                        pk=_row_pk(row, pk_cols) or {model_col: mid},
                        record=_row_to_record(row, columns),
                    ),
                    priority=4,
                )

    for candidate_row in candidates or []:
        _maybe_set_model_record(
            index,
            candidate_row.model_id,
            EntitySourceRecord(
                table_name="metrics_candidates",
                pk={"id": candidate_row.id},
                record=candidate_row.model_dump(),
            ),
            priority=5,
        )
        if candidate_row.dataset_id:
            index.model_to_dataset[candidate_row.model_id] = candidate_row.dataset_id
            index.dataset_to_models.setdefault(candidate_row.dataset_id, [])
            if candidate_row.model_id not in index.dataset_to_models[candidate_row.dataset_id]:
                index.dataset_to_models[candidate_row.dataset_id].append(candidate_row.model_id)

    return index


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


def _format_timestamp(value: Any) -> Optional[str]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def _record_created_at(record: dict[str, Any]) -> Optional[str]:
    preferred = ("__created_at", "created_at", "__create_ts", "create_ts", "__started_at", "started_at")
    for key, value in record.items():
        lowered = key.lower()
        if any(lowered == suffix or lowered.endswith(suffix) for suffix in preferred):
            ts = _format_timestamp(value)
            if ts:
                return ts
    return None


@dataclass
class _TrainingLinkContext:
    started_at: dict[tuple[str, str], str] = field(default_factory=dict)
    run_key: dict[tuple[str, str], str] = field(default_factory=dict)
    run_id: dict[tuple[str, str], str] = field(default_factory=dict)


def _load_training_link_context(ds: DataStore, catalog: Catalog) -> _TrainingLinkContext:
    ctx = _TrainingLinkContext()
    for name in catalog.catalog:
        if not is_training_status_table(name):
            continue
        dt = ds.get_table(name)
        try:
            df = dt.get_data()
        except Exception:
            continue
        if df.empty:
            continue
        columns = table_schema_columns(dt)
        model_col = model_id_column(columns)
        dataset_col = frozen_dataset_id_column(columns)
        if not model_col or not dataset_col:
            continue
        started_col = next((c for c in columns if "started_at" in c.lower()), None)
        run_key_col = next((c for c in columns if "run_key" in c.lower()), None)
        status_id_col = next((c for c in columns if c.endswith("_id") and "status" in c.lower()), None)
        for _, row in df.iterrows():
            model_value = row.get(model_col)
            dataset_value = row.get(dataset_col)
            if model_value is None or dataset_value is None:
                continue
            if isinstance(model_value, float) and pd.isna(model_value):
                continue
            if isinstance(dataset_value, float) and pd.isna(dataset_value):
                continue
            key = (str(model_value), str(dataset_value))
            if started_col:
                ts = _format_timestamp(row.get(started_col))
                if ts:
                    ctx.started_at.setdefault(key, ts)
            if run_key_col:
                rk = row.get(run_key_col)
                if rk is not None and not (isinstance(rk, float) and pd.isna(rk)):
                    ctx.run_key.setdefault(key, str(rk))
            if status_id_col:
                rid = row.get(status_id_col)
                if rid is not None and not (isinstance(rid, float) and pd.isna(rid)):
                    ctx.run_id.setdefault(key, str(rid))
    return ctx


def _build_linked_models_for_dataset(
    *,
    dataset_id: str,
    entity_index: EntitySourceIndex,
    training_ctx: _TrainingLinkContext,
    metrics_by_model: dict[str, MetricsModelRow],
) -> list[FrozenDatasetLinkedModelRow]:
    model_ids = list(entity_index.dataset_to_models.get(dataset_id, []))
    if not model_ids:
        model_ids = sorted(
            {
                model_id
                for model_id, linked_dataset_id in entity_index.model_to_dataset.items()
                if linked_dataset_id == dataset_id
            }
        )

    linked: list[FrozenDatasetLinkedModelRow] = []
    for model_id in model_ids:
        link = entity_index.dataset_model_link_records.get((dataset_id, model_id))
        link_record = link.record if link else None
        created_at = _record_created_at(link_record) if link_record else None
        train_key = (model_id, dataset_id)
        if not created_at:
            created_at = training_ctx.started_at.get(train_key)
        metrics_row = metrics_by_model.get(model_id)
        if not created_at and metrics_row:
            created_at = metrics_row.started_at
        run_key = training_ctx.run_key.get(train_key) or (metrics_row.run_key if metrics_row else None)
        run_id = training_ctx.run_id.get(train_key) or (metrics_row.run_id if metrics_row else None)
        linked.append(
            FrozenDatasetLinkedModelRow(
                model_id=model_id,
                created_at=created_at,
                run_key=run_key,
                run_id=run_id,
                link_table=link.table_name if link else None,
                link_record=link_record,
            )
        )
    linked.sort(key=lambda row: row.created_at or "", reverse=True)
    return linked


def _load_frozen_dataset_catalog(
    ds: Optional[DataStore],
    catalog: Optional[Catalog],
) -> tuple[list[FrozenDatasetRow], dict[str, FrozenDatasetRow], dict[str, str]]:
    datasets: list[FrozenDatasetRow] = []
    by_id: dict[str, FrozenDatasetRow] = {}
    model_to_dataset: dict[str, str] = {}

    if ds is None or catalog is None:
        return datasets, by_id, model_to_dataset

    for name in catalog.catalog:
        if is_frozen_dataset_table(name):
            dt = ds.get_table(name)
            try:
                df = dt.get_data()
            except Exception:
                continue
            if df.empty:
                continue
            columns = table_schema_columns(dt)
            id_col = frozen_dataset_id_column(columns)
            if not id_col:
                continue
            meta_cols = frozen_dataset_metadata_columns(columns)
            for _, row in df.iterrows():
                dataset_id = row.get(id_col)
                if dataset_id is None or (isinstance(dataset_id, float) and pd.isna(dataset_id)):
                    continue
                entry = FrozenDatasetRow(
                    dataset_id=str(dataset_id),
                    frozen_at=_format_timestamp(row.get(meta_cols.get("created_at")) if meta_cols.get("created_at") else None),
                    train_count=_safe_int(row.get(meta_cols.get("train_count")) if meta_cols.get("train_count") else None),
                    val_count=_safe_int(row.get(meta_cols.get("val_count")) if meta_cols.get("val_count") else None),
                    test_count=_safe_int(row.get(meta_cols.get("test_count")) if meta_cols.get("test_count") else None),
                )
                if entry.dataset_id not in by_id:
                    datasets.append(entry)
                    by_id[entry.dataset_id] = entry

        if is_model_frozen_dataset_link_table(name):
            dt = ds.get_table(name)
            try:
                df = dt.get_data()
            except Exception:
                continue
            if df.empty:
                continue
            columns = table_schema_columns(dt)
            model_col = model_id_column(columns)
            dataset_col = frozen_dataset_id_column(columns)
            if not model_col or not dataset_col:
                continue
            for _, row in df.iterrows():
                model_value = row.get(model_col)
                dataset_value = row.get(dataset_col)
                if model_value is None or dataset_value is None:
                    continue
                if isinstance(model_value, float) and pd.isna(model_value):
                    continue
                if isinstance(dataset_value, float) and pd.isna(dataset_value):
                    continue
                model_to_dataset[str(model_value)] = str(dataset_value)

    datasets.sort(key=lambda item: item.frozen_at or "", reverse=True)
    return datasets, by_id, model_to_dataset


def _enrich_runs_with_datasets(
    runs: list[MetricsRunRow],
    *,
    model_to_dataset: dict[str, str],
    datasets_by_id: dict[str, FrozenDatasetRow],
) -> list[MetricsRunRow]:
    enriched: list[MetricsRunRow] = []
    for run in runs:
        dataset_id = model_to_dataset.get(run.model_id)
        meta = datasets_by_id.get(dataset_id) if dataset_id else None
        enriched.append(
            run.model_copy(
                update={
                    "dataset_id": dataset_id,
                    "train_items": meta.train_count if meta else None,
                    "val_items": meta.val_count if meta else None,
                }
            )
        )
    return enriched


def _parse_model_ids(model_id: Optional[str]) -> Optional[list[str]]:
    if not model_id:
        return None
    ids = [part.strip() for part in model_id.split(",") if part.strip()]
    return ids or None


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
    model_ids = _parse_model_ids(model_id)
    if model_ids:
        allowed = set(model_ids)
        result = [r for r in result if r.model_id in allowed]
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

    # A metric field may be present only in some rows. Mixing floats (present) with
    # strings ("" fallback) breaks Python's comparison, so decide once whether we
    # are sorting a numeric metric and always emit a consistent, comparable key.
    # The direction is folded into the key (negated numbers / inverted char codes)
    # so that missing values (flagged with a leading 1) always sort last, in both
    # ascending and descending order.
    is_metric = any(sort_by in r.metrics for r in runs)

    def key_fn(r: MetricsRunRow) -> Any:
        if is_metric:
            val = r.metrics.get(sort_by)
            if val is None:
                return (1, 0.0)
            return (0, -float(val) if reverse else float(val))
        attr = r.model_dump().get(sort_by)
        if attr is None:
            return (1, ())
        if isinstance(attr, (int, float)):
            return (0, -float(attr) if reverse else float(attr))
        text = str(attr)
        return (0, tuple(-ord(c) for c in text) if reverse else tuple(ord(c) for c in text))

    return sorted(runs, key=key_fn)


def _model_row_id(model_id: str, dataset_id: str, subset: str) -> str:
    raw = f"{model_id}|{dataset_id}|{subset}"
    return hashlib.md5(raw.encode()).hexdigest()[:16]


def _has_computed_metrics(metrics: dict[str, float | None]) -> bool:
    return any(v is not None for k, v in metrics.items() if k not in _MODEL_COUNT_KEYS)


def _merge_run_metrics(group: list[MetricsRunRow]) -> dict[str, float | None]:
    merged: dict[str, float | None] = {}
    for row in group:
        for key, value in row.metrics.items():
            if value is not None and merged.get(key) is None:
                merged[key] = value
    return merged


def runs_to_model_rows(
    runs: list[MetricsRunRow],
    *,
    datasets_by_id: dict[str, FrozenDatasetRow],
    task_type: Optional[str],
    pipeline_id: str,
) -> list[MetricsModelRow]:
    groups: dict[tuple[str, str, str], list[MetricsRunRow]] = {}
    for row in runs:
        key = (row.model_id, row.dataset_id or "", row.subset)
        groups.setdefault(key, []).append(row)

    model_rows: list[MetricsModelRow] = []
    for (model_id, dataset_id, subset), group in groups.items():
        rep = max(group, key=lambda r: r.started_at or "")
        merged = _merge_run_metrics(group)
        dataset = datasets_by_id.get(dataset_id) if dataset_id else None
        has_metrics = _has_computed_metrics(merged)
        model_rows.append(
            MetricsModelRow(
                id=_model_row_id(model_id, dataset_id, subset),
                pipeline_id=pipeline_id,
                model_id=model_id,
                model_display_name=model_id,
                model_version=rep.model_version,
                task_type=rep.task_type or task_type,
                dataset_id=dataset_id or None,
                frozen_at=dataset.frozen_at if dataset else None,
                subset=subset,
                split_counts=SplitCounts(
                    train=dataset.train_count if dataset else rep.train_items,
                    val=dataset.val_count if dataset else rep.val_items,
                    test=dataset.test_count if dataset else None,
                ),
                has_metrics=has_metrics,
                metrics_state="computed" if has_metrics else "not_computed",
                metrics=merged,
                run_id=rep.run_id,
                run_key=rep.run_key,
                started_at=rep.started_at,
                finished_at=rep.finished_at,
                duration_s=rep.duration_s,
                status=rep.status,
                tags=rep.tags,
            )
        )
    return model_rows


def _merge_candidates(
    model_rows: list[MetricsModelRow],
    candidates: list[MetricsCandidateRow],
    datasets_by_id: dict[str, FrozenDatasetRow],
    pipeline_id: str,
) -> list[MetricsModelRow]:
    existing = {(r.model_id, r.dataset_id or "", r.subset) for r in model_rows}
    for candidate in candidates:
        key = (candidate.model_id, candidate.dataset_id, candidate.subset)
        if key in existing:
            continue
        dataset = datasets_by_id.get(candidate.dataset_id)
        model_rows.append(
            MetricsModelRow(
                id=candidate.id,
                pipeline_id=pipeline_id,
                model_id=candidate.model_id,
                model_display_name=candidate.model_id,
                model_source=candidate.model_source,
                task_type=candidate.task_type,
                dataset_id=candidate.dataset_id,
                frozen_at=dataset.frozen_at if dataset else None,
                subset=candidate.subset,
                split_counts=SplitCounts(
                    train=dataset.train_count if dataset else None,
                    val=dataset.val_count if dataset else None,
                    test=dataset.test_count if dataset else None,
                ),
                has_metrics=False,
                metrics_state=candidate.metrics_state,
                metrics={},
            )
        )
    return model_rows


def _filter_model_rows(
    rows: list[MetricsModelRow],
    *,
    subset: Optional[str] = None,
    model_id: Optional[str] = None,
    search: Optional[str] = None,
    task_type: Optional[str] = None,
) -> list[MetricsModelRow]:
    result = rows
    if subset:
        result = [r for r in result if r.subset == subset]
    model_ids = _parse_model_ids(model_id)
    if model_ids:
        allowed = set(model_ids)
        result = [r for r in result if r.model_id in allowed]
    if task_type and task_type != "auto":
        result = [r for r in result if (r.task_type or "").lower() == task_type.lower()]
    if search:
        q = search.lower()
        result = [
            r
            for r in result
            if q in r.model_id.lower()
            or q in (r.dataset_id or "").lower()
            or q in r.subset.lower()
            or any(q in tag.lower() for tag in r.tags)
        ]
    return result


def _sort_model_rows(rows: list[MetricsModelRow], sort_by: Optional[str], sort_dir: str) -> list[MetricsModelRow]:
    if not sort_by:
        return rows
    reverse = sort_dir != "asc"
    is_metric = any(sort_by in r.metrics for r in rows)

    def key_fn(row: MetricsModelRow) -> Any:
        if is_metric:
            val = row.metrics.get(sort_by)
            if val is None:
                return (1, 0.0)
            return (0, -float(val) if reverse else float(val))
        attr = row.model_dump().get(sort_by)
        if attr is None:
            return (1, ())
        if isinstance(attr, (int, float)):
            return (0, -float(attr) if reverse else float(attr))
        text = str(attr)
        return (0, tuple(-ord(c) for c in text) if reverse else tuple(ord(c) for c in text))

    return sorted(rows, key=key_fn)


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
        val = r.model_dump().get(field)
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


def _build_model_detail_kpis(row: Optional[MetricsModelRow]) -> list[MetricsModelDetailKpi]:
    if row is None or not row.has_metrics:
        return []
    metrics = row.metrics or {}
    specs = [
        ("weighted_f1_score", "W-F1"),
        ("macro_f1_score", "M-F1"),
        ("weighted_without_pseudo_classes_precision", "W-Precision"),
        ("weighted_without_pseudo_classes_recall", "W-Recall"),
        ("weighted_precision", "W-Precision"),
        ("weighted_recall", "W-Recall"),
        ("support", "Support"),
    ]
    seen: set[str] = set()
    kpis: list[MetricsModelDetailKpi] = []
    for key, label in specs:
        if key in seen:
            continue
        val = metrics.get(key)
        if val is None:
            continue
        seen.add(key)
        fmt = "integer" if key == "support" else "float"
        kpis.append(MetricsModelDetailKpi(key=key, label=label, value=val, format=fmt))
    return kpis


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

    def _dataset_context(self) -> tuple[dict[str, FrozenDatasetRow], dict[str, str]]:
        _, by_id, model_to_dataset = _load_frozen_dataset_catalog(self.ds, self.catalog)
        return by_id, model_to_dataset

    def _all_runs(self, pipeline_id: str) -> tuple[list[MetricsRunRow], Optional[str]]:
        catalog_runs, _, task_type = _load_catalog_rows(self.ds, self.catalog)
        datasets_by_id, model_to_dataset = self._dataset_context()
        runs = _enrich_runs_with_datasets(
            catalog_runs,
            model_to_dataset=model_to_dataset,
            datasets_by_id=datasets_by_id,
        )
        for r in runs:
            r.pipeline_id = pipeline_id
        for i, run in enumerate(runs):
            if i + 1 < len(runs):
                compute_deltas(run, runs[i + 1])
        return runs, task_type

    def _all_model_rows(self, pipeline_id: str) -> tuple[list[MetricsModelRow], Optional[str]]:
        runs, discovered_task_type = self._all_runs(pipeline_id)
        datasets_by_id, _ = self._dataset_context()
        model_rows = runs_to_model_rows(
            runs,
            datasets_by_id=datasets_by_id,
            task_type=discovered_task_type,
            pipeline_id=pipeline_id,
        )
        model_rows = _merge_candidates(
            model_rows,
            _load_candidates(self.store, pipeline_id),
            datasets_by_id,
            pipeline_id,
        )
        return model_rows, discovered_task_type

    def list_frozen_datasets(self, pipeline_id: str) -> FrozenDatasetsResponse:
        rows, by_id, model_to_dataset = _load_frozen_dataset_catalog(self.ds, self.catalog)
        candidates = _load_candidates(self.store, pipeline_id)
        entity_index = _load_entity_source_records(self.ds, self.catalog, candidates=candidates)
        model_rows, _ = self._all_model_rows(pipeline_id)
        counts: dict[str, set[str]] = {}
        for model_id, dataset_id in {**model_to_dataset, **entity_index.model_to_dataset}.items():
            counts.setdefault(dataset_id, set()).add(model_id)
        for row in model_rows:
            if row.dataset_id:
                counts.setdefault(row.dataset_id, set()).add(row.model_id)
        enriched: list[FrozenDatasetRow] = []
        for frozen_row in rows:
            enriched.append(
                frozen_row.model_copy(update={"models_count": len(counts.get(frozen_row.dataset_id, set()))}),
            )
        return FrozenDatasetsResponse(rows=enriched, total=len(enriched))

    def list_runs(
        self,
        pipeline_id: str,
        *,
        subset: Optional[str] = None,
        model_id: Optional[str] = None,
        search: Optional[str] = None,
        task_type: Optional[str] = None,
        from_dt: Optional[datetime] = None,
        to_dt: Optional[datetime] = None,
        sort_by: Optional[str] = None,
        sort_dir: str = "desc",
        limit: int = 25,
        offset: int = 0,
    ) -> MetricsRunsResponse:
        runs, discovered_task_type = self._all_runs(pipeline_id)
        runs = _filter_runs(runs, subset=subset, model_id=model_id, search=search, from_dt=from_dt, to_dt=to_dt)
        datasets_by_id, _ = self._dataset_context()
        model_rows = runs_to_model_rows(
            runs,
            datasets_by_id=datasets_by_id,
            task_type=discovered_task_type,
            pipeline_id=pipeline_id,
        )
        model_rows = _merge_candidates(
            model_rows,
            _load_candidates(self.store, pipeline_id),
            datasets_by_id,
            pipeline_id,
        )
        model_rows = _filter_model_rows(
            model_rows,
            subset=subset,
            model_id=model_id,
            search=search,
            task_type=task_type,
        )
        model_rows = _sort_model_rows(model_rows, sort_by or "model_id", sort_dir)
        total = len(model_rows)
        page = model_rows[offset : offset + limit]
        subsets = sorted({r.subset for r in model_rows if r.subset})
        models = sorted({r.model_id for r in model_rows if r.model_id})
        metric_keys = sorted({k for r in model_rows for k in r.metrics})
        schema = build_metric_schema(
            discovered_task_type,
            metric_keys,
            primary_metric=primary_metric_for_task(discovered_task_type),
        )
        return MetricsRunsResponse(
            rows=page,
            total=total,
            available_filters={"subsets": subsets, "models": models, "tags": [], "metrics": metric_keys},
            schema=schema,
        )

    def get_schema(self, pipeline_id: str, *, task_type: Optional[str] = None) -> MetricsTableSchema:
        runs, discovered_task_type = self._all_runs(pipeline_id)
        metric_keys = sorted({k for r in runs for k in r.metrics})
        tt = task_type or discovered_task_type
        return build_metric_schema(tt, metric_keys, primary_metric=primary_metric_for_task(tt))

    def list_candidates(self, pipeline_id: str) -> MetricsCandidatesResponse:
        rows = _load_candidates(self.store, pipeline_id)
        return MetricsCandidatesResponse(rows=rows, total=len(rows))

    def add_candidate(self, pipeline_id: str, body: MetricsCandidateCreate) -> MetricsCandidateRow:
        if self.store is None:
            raise ValueError("Observability store is required for metrics candidates")
        existing = self.store.list_metrics_candidates(pipeline_id)
        candidate_id = hashlib.md5(
            f"{body.model_id}|{body.dataset_id}|{body.subset}|{len(existing)}".encode()
        ).hexdigest()[:16]
        saved = self.store.add_metrics_candidate(
            PipelineMetricsCandidateRow(
                id=candidate_id,
                pipeline_id=pipeline_id,
                model_id=body.model_id,
                model_source=body.model_source,
                artifact_uri=body.artifact_uri,
                dataset_id=body.dataset_id,
                subset=body.subset,
                task_type=body.task_type,
                metrics_state="not_computed",
                created_at=utc_now(),
            )
        )
        return _candidate_row_from_db(saved)

    def delete_candidate(self, pipeline_id: str, candidate_id: str) -> bool:
        if self.store is None:
            return False
        return self.store.delete_metrics_candidate(pipeline_id, candidate_id)

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
        # Metrics are stored sparsely: each evaluation row of a (model, subset)
        # may carry only a slice of the computed metrics. Merge every non-null
        # metric from all rows of the best run's (model, subset) so the Best model
        # panel presents the complete metric set rather than one sparse row.
        best = best.model_copy(deep=True)
        merged: dict[str, float | None] = dict(best.metrics)
        for r in runs:
            if r.model_id != best.model_id or r.subset != best.subset:
                continue
            for k, v in r.metrics.items():
                if v is not None and merged.get(k) is None:
                    merged[k] = v
        best.metrics = merged
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
        series: list[dict[str, object]] = []
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

    def get_model_detail(
        self,
        pipeline_id: str,
        model_id: str,
        *,
        dataset_id: Optional[str] = None,
        subset: Optional[str] = None,
    ) -> MetricsModelDetailResponse:
        model_rows, _ = self._all_model_rows(pipeline_id)
        matching = [r for r in model_rows if r.model_id == model_id]
        if dataset_id:
            matching = [r for r in matching if r.dataset_id == dataset_id]
        if subset:
            matching = [r for r in matching if r.subset == subset]

        entity_index = _load_entity_source_records(
            self.ds,
            self.catalog,
            candidates=_load_candidates(self.store, pipeline_id),
        )
        datasets_by_id, _ = self._dataset_context()
        source = entity_index.model_records_by_id.get(model_id)
        linked_dataset_id = (
            dataset_id
            or (matching[0].dataset_id if matching else None)
            or entity_index.model_to_dataset.get(model_id)
        )
        frozen = datasets_by_id.get(linked_dataset_id) if linked_dataset_id else None
        dataset_source = entity_index.dataset_records_by_id.get(linked_dataset_id) if linked_dataset_id else None
        primary_row = next((r for r in matching if r.has_metrics), matching[0] if matching else None)
        model_col = "model_id"
        if source:
            model_col = model_id_column(list(source.record.keys())) or model_col
        dataset_col = "frozen_dataset_id"
        if dataset_source:
            dataset_col = frozen_dataset_id_column(list(dataset_source.record.keys())) or dataset_col

        return MetricsModelDetailResponse(
            pipeline_id=pipeline_id,
            model_id=model_id,
            title=model_id,
            source_table=source.table_name if source else None,
            source_pk=source.pk if source else None,
            source_record=source.record if source else None,
            source_table_url=_table_row_url(pipeline_id, source.table_name if source else None, model_col, model_id),
            model_row=primary_row,
            frozen_dataset=frozen,
            frozen_dataset_source_table=dataset_source.table_name if dataset_source else None,
            frozen_dataset_source_pk=dataset_source.pk if dataset_source else None,
            metrics_rows=matching,
            kpis=_build_model_detail_kpis(primary_row),
            related=MetricsModelDetailRelated(
                dataset_id=linked_dataset_id,
                run_key=primary_row.run_key if primary_row else None,
                run_id=primary_row.run_id if primary_row else None,
            ),
        )

    def get_frozen_dataset_detail(
        self,
        pipeline_id: str,
        dataset_id: str,
        *,
        subset: Optional[str] = None,
    ) -> FrozenDatasetDetailResponse:
        rows, by_id, _ = _load_frozen_dataset_catalog(self.ds, self.catalog)
        dataset = by_id.get(dataset_id) or FrozenDatasetRow(dataset_id=dataset_id)
        entity_index = _load_entity_source_records(
            self.ds,
            self.catalog,
            candidates=_load_candidates(self.store, pipeline_id),
        )
        source = entity_index.dataset_records_by_id.get(dataset_id)
        model_rows, task_type = self._all_model_rows(pipeline_id)
        metrics_by_model: dict[str, MetricsModelRow] = {}
        for row in model_rows:
            if row.dataset_id != dataset_id:
                continue
            existing = metrics_by_model.get(row.model_id)
            if existing is None or (row.started_at or "") > (existing.started_at or ""):
                metrics_by_model[row.model_id] = row

        training_ctx = (
            _load_training_link_context(self.ds, self.catalog)
            if self.ds is not None and self.catalog is not None
            else _TrainingLinkContext()
        )
        linked_models = _build_linked_models_for_dataset(
            dataset_id=dataset_id,
            entity_index=entity_index,
            training_ctx=training_ctx,
            metrics_by_model=metrics_by_model,
        )

        linked_model_ids = {row.model_id for row in linked_models}
        models_count = len(linked_models)
        with_metrics = len(
            {
                model_id
                for model_id in linked_model_ids
                if metrics_by_model.get(model_id) and metrics_by_model[model_id].has_metrics
            }
        )
        subsets = sorted({row.subset for row in model_rows if row.model_id in linked_model_ids and row.subset})
        best_key = primary_metric_for_task(task_type)
        best_row = max(
            (metrics_by_model[mid] for mid in linked_model_ids if mid in metrics_by_model and metrics_by_model[mid].has_metrics),
            key=lambda r: pick_primary_value(r.metrics, task_type) or -1,
            default=None,
        )
        dataset_col = "frozen_dataset_id"
        if source:
            dataset_col = frozen_dataset_id_column(list(source.record.keys())) or dataset_col

        return FrozenDatasetDetailResponse(
            pipeline_id=pipeline_id,
            dataset_id=dataset_id,
            title=dataset_id,
            dataset=dataset.model_copy(update={"models_count": models_count or dataset.models_count}),
            source_table=source.table_name if source else None,
            source_pk=source.pk if source else None,
            source_record=source.record if source else None,
            source_table_url=_table_row_url(pipeline_id, source.table_name if source else None, dataset_col, dataset_id),
            linked_models=linked_models,
            coverage=FrozenDatasetCoverage(
                models_total=models_count,
                models_with_metrics=with_metrics,
                subsets=subsets,
                best_metric_key=best_key if best_row else None,
                best_metric_value=pick_primary_value(best_row.metrics, task_type) if best_row else None,
                best_model_id=best_row.model_id if best_row else None,
            ),
        )
