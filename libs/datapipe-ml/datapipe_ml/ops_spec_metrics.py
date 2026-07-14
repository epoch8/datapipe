from __future__ import annotations

from typing import Any, Optional

import pandas as pd
from datapipe.compute import Catalog
from datapipe.datatable import DataStore

from datapipe_app.ops_query import OpsQuery, metric_table_columns
from datapipe_ml.spec_registry import OpsSpecRegistry, _flatten_columns
from datapipe_app.specs import OpsColumn, OpsMetricTableSpec


def _column_source(table_spec: OpsMetricTableSpec, column_id: str) -> str | None:
    for column in [
        *table_spec.primary_columns,
        *_flatten_columns(table_spec.metric_columns),
        *table_spec.filters,
    ]:
        if column.id == column_id:
            return column.source
    if column_id in {column.source for column in table_spec.primary_columns}:
        return column_id
    return None


def _metric_display_name(table_spec: OpsMetricTableSpec, metric_source: str) -> str:
    for column in _flatten_columns(table_spec.metric_columns):
        if column.source == metric_source:
            return column.id
    return metric_source


def _default_filters(table_spec: OpsMetricTableSpec) -> dict[str, str]:
    filters: dict[str, str] = {}
    for rule in table_spec.default_filters:
        if rule.operator != "equal" or rule.value is None:
            continue
        source = _column_source(table_spec, rule.column_id)
        if source:
            filters[source] = rule.value
    return filters


def _metric_value(row: dict[str, Any], column: str) -> float | None:
    value = row.get(column)
    if value is None or (hasattr(value, "__float__") and pd.isna(value)):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _metric_query_columns(table_spec: OpsMetricTableSpec) -> list[OpsColumn]:
    columns = metric_table_columns(table_spec)
    seen_sources = {column.source for column in columns}
    for source in table_spec.entity_links.values():
        if source not in seen_sources:
            columns.append(OpsColumn(source, source, source, filterable=True))
            seen_sources.add(source)
    if table_spec.best_metric_column and table_spec.best_metric_column not in seen_sources:
        display = _metric_display_name(table_spec, table_spec.best_metric_column)
        columns.append(
            OpsColumn(display, display, table_spec.best_metric_column)
        )
    return columns


def _iter_metric_tables(registry: OpsSpecRegistry):
    for spec in registry.list():
        for table_spec in spec.metrics:
            yield spec, table_spec


def latest_eval_metric_from_specs(
    registry: OpsSpecRegistry,
    ds: DataStore,
    catalog: Catalog,
    *,
    model_id: str | None = None,
) -> dict[str, Any] | None:
    if not registry.list():
        return None

    query = OpsQuery(ds, catalog)
    for spec, table_spec in _iter_metric_tables(registry):
        metric_source = table_spec.best_metric_column
        if not metric_source:
            continue

        filters = _default_filters(table_spec)
        if model_id is not None and "model" in table_spec.entity_links:
            filters[table_spec.entity_links["model"]] = model_id

        sort_by: str | None = None
        sort_dir = "desc"
        if table_spec.default_sort:
            sort_key, sort_dir = table_spec.default_sort[0]
            sort_by = _column_source(table_spec, sort_key) or sort_key

        rows, _total = query.rows(
            table_spec.table,
            allowed_columns=_metric_query_columns(table_spec),
            filters=filters or None,
            sort_by=sort_by,
            sort_dir=sort_dir,
            limit=1,
            offset=0,
        )
        if not rows:
            continue

        row = rows[0]
        metric_value = _metric_value(row, metric_source)
        if metric_value is None:
            continue

        model_col = table_spec.entity_links.get("model")
        resolved_model_id = None
        if model_col and row.get(model_col) is not None and not pd.isna(row.get(model_col)):
            resolved_model_id = str(row.get(model_col))

        return {
            "spec_id": spec.id,
            "metric_table_id": table_spec.id,
            "model_id": resolved_model_id,
            "metric_name": _metric_display_name(table_spec, metric_source),
            "metric_column": metric_source,
            "metric_value": metric_value,
            "source_table": table_spec.table,
        }

    return None
