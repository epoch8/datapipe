from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, get_args, get_origin

import pandas as pd
from datapipe.compute import Catalog
from datapipe.datatable import DataStore

from datapipe_app_ml_ops.observability.schemas.models import (
    ClassMetricRow,
    EntitySourceRecord,
    FrozenDatasetRow,
    MetricsCandidateRow,
    MetricsRunRow,
)
from datapipe_app_ml_ops.observability.metrics.spec_catalog_utils import (
    format_timestamp,
    row_pk,
    row_to_record,
    safe_float,
    safe_int,
    table_columns,
)
from datapipe_app.ops.ops_spec_resolve import column_source_by_link
from datapipe_app_ml_ops.ops.spec_registry import OpsSpecRegistry, _flatten_columns
from datapipe_app.ops.specs import OpsMetricTableSpec, OpsRelationSpec


def task_type_from_specs(registry: OpsSpecRegistry) -> str | None:
    for spec in registry.list():
        if spec.tags:
            return spec.tags[0]
        return spec.id
    return None


def primary_metric_from_specs(registry: OpsSpecRegistry) -> str | None:
    for spec in registry.list():
        for table in spec.metrics:
            if table.best_metric_column:
                for column in _flatten_columns(table.metric_columns):
                    if column.source == table.best_metric_column:
                        return column.id
                return table.best_metric_column
    return None


def _integer_metric_ids() -> set[str]:
    ids: set[str] = set()
    for name, info in ClassMetricRow.model_fields.items():
        annotation = info.annotation
        if annotation is int:
            ids.add(name)
            continue
        origin = get_origin(annotation)
        if origin is not None:
            if int in get_args(annotation):
                ids.add(name)
    return ids


_INTEGER_METRIC_IDS = _integer_metric_ids()


@dataclass
class EntitySourceIndex:
    model_records_by_id: dict[str, EntitySourceRecord] = field(default_factory=dict)
    dataset_records_by_id: dict[str, EntitySourceRecord] = field(default_factory=dict)
    model_to_dataset: dict[str, str] = field(default_factory=dict)
    dataset_to_models: dict[str, list[str]] = field(default_factory=dict)
    dataset_model_link_records: dict[tuple[str, str], EntitySourceRecord] = field(default_factory=dict)
    model_to_spec: dict[str, str] = field(default_factory=dict)
    dataset_to_spec: dict[str, str] = field(default_factory=dict)
    _model_priority: dict[str, int] = field(default_factory=dict)
    _dataset_priority: dict[str, int] = field(default_factory=dict)


@dataclass
class TrainingLinkContext:
    started_at: dict[tuple[str, str], str] = field(default_factory=dict)
    run_key: dict[tuple[str, str], str] = field(default_factory=dict)
    run_id: dict[tuple[str, str], str] = field(default_factory=dict)


def _maybe_set_model_record(index: EntitySourceIndex, model_id: str, record: EntitySourceRecord, priority: int) -> None:
    if model_id not in index.model_records_by_id or index._model_priority.get(model_id, 99) > priority:
        index.model_records_by_id[model_id] = record
        index._model_priority[model_id] = priority


def _maybe_set_dataset_record(index: EntitySourceIndex, dataset_id: str, record: EntitySourceRecord, priority: int) -> None:
    if dataset_id not in index.dataset_records_by_id or index._dataset_priority.get(dataset_id, 99) > priority:
        index.dataset_records_by_id[dataset_id] = record
        index._dataset_priority[dataset_id] = priority


def _metric_sources(table_spec: OpsMetricTableSpec) -> list[tuple[str, str]]:
    return [(column.id, column.source) for column in _flatten_columns(table_spec.metric_columns)]


def _row_metrics(row: pd.Series, table_spec: OpsMetricTableSpec) -> dict[str, float | None]:
    metrics: dict[str, float | None] = {}
    for metric_id, source in _metric_sources(table_spec):
        parsed = safe_float(row.get(source))
        if parsed is not None:
            metrics[metric_id] = parsed
    return metrics


def _entity_link_value(row: pd.Series, entity_links: dict[str, str], entity: str) -> str:
    column = entity_links.get(entity)
    if not column:
        return ""
    value = row.get(column)
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    return str(value)


def load_metric_runs_from_specs(
    registry: OpsSpecRegistry,
    ds: DataStore,
    catalog: Catalog,
) -> tuple[list[MetricsRunRow], str | None]:
    run_rows: list[MetricsRunRow] = []
    task_type = task_type_from_specs(registry)

    for spec in registry.list():
        for table_spec in spec.metrics:
            entity_links = table_spec.entity_links
            dt = ds.get_table(table_spec.table)
            try:
                df = dt.get_data()
            except Exception:
                continue
            if df.empty:
                continue
            for idx, row in df.iterrows():
                model_id = _entity_link_value(row, entity_links, "model")
                subset = _entity_link_value(row, entity_links, "subset")
                metrics = _row_metrics(row, table_spec)
                if not metrics:
                    continue
                run_rows.append(
                    MetricsRunRow(
                        run_id=f"{table_spec.id}:{model_id}:{subset}:{idx}",
                        pipeline_id="",
                        model_id=model_id,
                        task_type=task_type,
                        subset=subset,
                        status="success",
                        metrics=metrics,
                    )
                )

    return run_rows, task_type


def _class_metric_row(row: pd.Series, table_spec: OpsMetricTableSpec, *, model_id: str, subset: str | None) -> ClassMetricRow | None:
    class_col = table_spec.entity_links.get("class") or table_spec.entity_links.get("tag")
    if not class_col:
        return None
    label = row.get(class_col)
    if label is None or (isinstance(label, float) and pd.isna(label)):
        return None
    payload: dict[str, Any] = {
        "label": str(label),
        "subset": subset,
        "model_id": model_id or None,
    }
    field_names = set(ClassMetricRow.model_fields)
    for metric_id, source in _metric_sources(table_spec):
        if metric_id not in field_names:
            continue
        if metric_id in _INTEGER_METRIC_IDS:
            payload[metric_id] = safe_int(row.get(source))
        else:
            payload[metric_id] = safe_float(row.get(source))
    if "class_id" in table_spec.primary_key_columns:
        payload["class_id"] = row.get(next(iter(table_spec.primary_key_columns), ""))
    return ClassMetricRow(**payload)


def _relation_for(registry: OpsSpecRegistry, relation_id: str | None) -> OpsRelationSpec | None:
    if not relation_id:
        return None
    for spec in registry.list():
        for relation in spec.relations:
            if relation.id == relation_id:
                return relation
    return None


def _index_frozen_datasets(index: EntitySourceIndex, registry: OpsSpecRegistry, ds: DataStore) -> None:
    for spec in registry.list():
        frozen = spec.frozen_dataset
        if frozen is None:
            continue
        dt = ds.get_table(frozen.table)
        columns = table_columns(dt)
        pk_cols = list(dt.primary_keys) or [frozen.id_column]
        try:
            df = dt.get_data()
        except Exception:
            continue
        for _, row in df.iterrows():
            dataset_id = row.get(frozen.id_column)
            if dataset_id is None or pd.isna(dataset_id):
                continue
            did = str(dataset_id)
            _maybe_set_dataset_record(
                index,
                did,
                EntitySourceRecord(
                    table_name=frozen.table,
                    pk=row_pk(row, pk_cols) or {frozen.id_column: did},
                    record=row_to_record(row, columns),
                ),
                priority=1,
            )
            index.dataset_to_spec[did] = spec.id


def _index_models(index: EntitySourceIndex, registry: OpsSpecRegistry, ds: DataStore) -> None:
    for spec in registry.list():
        model = spec.model
        if model is None:
            continue
        dt = ds.get_table(model.table)
        columns = table_columns(dt)
        pk_cols = list(dt.primary_keys) or [model.id_column]
        try:
            df = dt.get_data()
        except Exception:
            continue
        for _, row in df.iterrows():
            model_id = row.get(model.id_column)
            if model_id is None or pd.isna(model_id):
                continue
            mid = str(model_id)
            _maybe_set_model_record(
                index,
                mid,
                EntitySourceRecord(
                    table_name=model.table,
                    pk=row_pk(row, pk_cols) or {model.id_column: mid},
                    record=row_to_record(row, columns),
                ),
                priority=1,
            )
            index.model_to_spec[mid] = spec.id


def _index_relations(index: EntitySourceIndex, registry: OpsSpecRegistry, ds: DataStore) -> None:
    for spec in registry.list():
        for relation in spec.relations:
            dt = ds.get_table(relation.table)
            columns = table_columns(dt)
            pk_cols = list(dt.primary_keys)
            try:
                df = dt.get_data()
            except Exception:
                continue
            for _, row in df.iterrows():
                model_value = row.get(relation.from_column)
                dataset_value = row.get(relation.to_column)
                if model_value is None or dataset_value is None or pd.isna(model_value) or pd.isna(dataset_value):
                    continue
                mid, did = str(model_value), str(dataset_value)
                index.model_to_dataset[mid] = did
                index.dataset_to_models.setdefault(did, [])
                if mid not in index.dataset_to_models[did]:
                    index.dataset_to_models[did].append(mid)
                record = EntitySourceRecord(
                    table_name=relation.table,
                    pk=row_pk(row, pk_cols),
                    record=row_to_record(row, columns),
                )
                index.dataset_model_link_records[(did, mid)] = record
                _maybe_set_model_record(index, mid, record, priority=2)
                _maybe_set_dataset_record(index, did, record, priority=3)


def _index_metric_tables(index: EntitySourceIndex, registry: OpsSpecRegistry, ds: DataStore) -> None:
    for spec in registry.list():
        for table_spec in [*spec.metrics, *spec.class_metrics]:
            model_col = table_spec.entity_links.get("model")
            if not model_col:
                continue
            dt = ds.get_table(table_spec.table)
            columns = table_columns(dt)
            pk_cols = list(dt.primary_keys) or list(table_spec.primary_key_columns)
            try:
                df = dt.get_data()
            except Exception:
                continue
            for _, row in df.iterrows():
                model_value = row.get(model_col)
                if model_value is None or pd.isna(model_value):
                    continue
                mid = str(model_value)
                _maybe_set_model_record(
                    index,
                    mid,
                    EntitySourceRecord(
                        table_name=table_spec.table,
                        pk=row_pk(row, pk_cols) or {model_col: mid},
                        record=row_to_record(row, columns),
                    ),
                    priority=4,
                )


def _index_candidates(index: EntitySourceIndex, candidates: list[MetricsCandidateRow] | None) -> None:
    for candidate_row in candidates or []:
        model_id = candidate_row.model_id
        if not model_id:
            continue
        _maybe_set_model_record(
            index,
            model_id,
            EntitySourceRecord(
                table_name="metrics_candidates",
                pk={"id": candidate_row.id},
                record=candidate_row.model_dump(),
            ),
            priority=5,
        )
        dataset_id = candidate_row.dataset_id
        if dataset_id:
            index.model_to_dataset[model_id] = dataset_id
            index.dataset_to_models.setdefault(dataset_id, [])
            if model_id not in index.dataset_to_models[dataset_id]:
                index.dataset_to_models[dataset_id].append(model_id)


def load_entity_index_from_specs(
    registry: OpsSpecRegistry,
    ds: DataStore,
    catalog: Catalog,
    *,
    candidates: list[MetricsCandidateRow] | None = None,
) -> EntitySourceIndex:
    index = EntitySourceIndex()
    _index_frozen_datasets(index, registry, ds)
    _index_models(index, registry, ds)
    _index_relations(index, registry, ds)
    _index_metric_tables(index, registry, ds)
    _index_candidates(index, candidates)
    return index


def load_frozen_dataset_catalog_from_specs(
    registry: OpsSpecRegistry,
    ds: DataStore,
    catalog: Catalog,
) -> tuple[list[FrozenDatasetRow], dict[str, FrozenDatasetRow], dict[str, str]]:
    datasets: list[FrozenDatasetRow] = []
    by_id: dict[str, FrozenDatasetRow] = {}
    model_to_dataset: dict[str, str] = {}

    for spec in registry.list():
        frozen = spec.frozen_dataset
        if frozen is None:
            continue
        dt = ds.get_table(frozen.table)
        try:
            df = dt.get_data()
        except Exception:
            continue
        if df.empty:
            continue
        relation = _relation_for(registry, frozen.models_count_relation_id)
        for _, row in df.iterrows():
            dataset_id = row.get(frozen.id_column)
            if dataset_id is None or pd.isna(dataset_id):
                continue
            entry = FrozenDatasetRow(
                dataset_id=str(dataset_id),
                frozen_at=format_timestamp(row.get(frozen.created_at_column)),
                train_count=safe_int(row.get(frozen.split_columns.get("train"))) if frozen.split_columns.get("train") else None,
                val_count=safe_int(row.get(frozen.split_columns.get("val"))) if frozen.split_columns.get("val") else None,
                test_count=safe_int(row.get(frozen.split_columns.get("test"))) if frozen.split_columns.get("test") else None,
            )
            if entry.dataset_id not in by_id:
                datasets.append(entry)
                by_id[entry.dataset_id] = entry

        if relation is not None:
            link_dt = ds.get_table(relation.table)
            try:
                link_df = link_dt.get_data()
            except Exception:
                continue
            for _, row in link_df.iterrows():
                model_value = row.get(relation.from_column)
                dataset_value = row.get(relation.to_column)
                if model_value is None or dataset_value is None or pd.isna(model_value) or pd.isna(dataset_value):
                    continue
                model_to_dataset[str(model_value)] = str(dataset_value)

    datasets.sort(key=lambda item: item.frozen_at or "", reverse=True)
    return datasets, by_id, model_to_dataset


def load_class_metric_rows_from_specs(
    registry: OpsSpecRegistry,
    ds: DataStore,
    catalog: Catalog,
) -> list[tuple[str, ClassMetricRow, str]]:
    class_rows: list[tuple[str, ClassMetricRow, str]] = []
    for spec in registry.list():
        for table_spec in spec.class_metrics:
            model_col = table_spec.entity_links.get("model")
            subset_col = table_spec.entity_links.get("subset")
            dt = ds.get_table(table_spec.table)
            try:
                df = dt.get_data()
            except Exception:
                continue
            for _, row in df.iterrows():
                model_id = str(row.get(model_col) or "") if model_col else ""
                subset = str(row.get(subset_col) or "") if subset_col and not pd.isna(row.get(subset_col)) else None
                class_row = _class_metric_row(row, table_spec, model_id=model_id, subset=subset)
                if class_row is not None:
                    class_rows.append((table_spec.table, class_row, subset or ""))
    return class_rows


def load_catalog_rows_from_specs(
    registry: OpsSpecRegistry,
    ds: DataStore,
    catalog: Catalog,
) -> tuple[list[MetricsRunRow], list[tuple[str, ClassMetricRow, str]], str | None]:
    run_rows, task_type = load_metric_runs_from_specs(registry, ds, catalog)
    class_rows = load_class_metric_rows_from_specs(registry, ds, catalog)
    return run_rows, class_rows, task_type


def load_training_link_context_from_specs(
    registry: OpsSpecRegistry,
    ds: DataStore,
    catalog: Catalog,
) -> TrainingLinkContext:
    ctx = TrainingLinkContext()
    for spec in registry.list():
        training = spec.training
        if training is None:
            continue
        model_col = column_source_by_link(training.columns, "model")
        dataset_col = column_source_by_link(training.columns, "frozen_dataset")
        run_col = column_source_by_link(training.columns, "training_run")
        started_col = next((column.source for column in training.columns if column.kind == "datetime"), None)
        status_id_col = next((column.source for column in training.columns if column.source.endswith("_id")), None)
        if not model_col or not dataset_col:
            continue
        dt = ds.get_table(training.status_table)
        try:
            df = dt.get_data()
        except Exception:
            continue
        for _, row in df.iterrows():
            model_value = row.get(model_col)
            dataset_value = row.get(dataset_col)
            if model_value is None or dataset_value is None or pd.isna(model_value) or pd.isna(dataset_value):
                continue
            key = (str(model_value), str(dataset_value))
            if started_col:
                started = format_timestamp(row.get(started_col))
                if started:
                    ctx.started_at[key] = started
            if run_col:
                run_key = row.get(run_col)
                if run_key is not None and not pd.isna(run_key):
                    ctx.run_key[key] = str(run_key)
            if status_id_col:
                status_id = row.get(status_id_col)
                if status_id is not None and not pd.isna(status_id):
                    ctx.run_id[key] = str(status_id)
    return ctx


__all__ = [
    "EntitySourceIndex",
    "TrainingLinkContext",
    "load_catalog_rows_from_specs",
    "load_metric_runs_from_specs",
    "load_class_metric_rows_from_specs",
    "load_entity_index_from_specs",
    "load_frozen_dataset_catalog_from_specs",
    "load_training_link_context_from_specs",
    "primary_metric_from_specs",
]
