from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

from sqlalchemy import func, select

from datapipe.compute import Catalog
from datapipe.datatable import DataStore
from datapipe_app_ml_ops.ops.ops_specs import DatapipeOpsSpec
from datapipe_app_ml_ops.ops.spec_registry import OpsSpecRegistry

DATASOURCE = "datapipe_analytics"


@dataclass(frozen=True)
class SqlAnalyticsContext:
    qualified_table_map: dict[str, str]
    bare_table_map: dict[str, str]
    schema_tables: list[dict[str, Any]]


def _qualified_table(table: str, schema: str | None) -> str:
    if schema:
        return f'"{schema}"."{table}"'
    return f'"{table}"'


def _pick_metric_table(registry: OpsSpecRegistry) -> str | None:
    for spec in registry.list():
        for metric in spec.metrics:
            if metric.id == "model_metrics":
                return metric.table
    for spec in registry.list():
        if spec.metrics:
            return spec.metrics[0].table
    return None


def _pick_class_metric_table(registry: OpsSpecRegistry) -> str | None:
    for spec in registry.list():
        for metric in spec.class_metrics:
            if metric.id == "subset_class_metrics":
                return metric.table
    for spec in registry.list():
        if spec.class_metrics:
            return spec.class_metrics[0].table
    return None


def _pick_training_table(registry: OpsSpecRegistry) -> str | None:
    for spec in registry.list():
        if spec.training is not None:
            return spec.training.status_table
    return None


def _pick_model_table(registry: OpsSpecRegistry) -> str | None:
    for spec in registry.list():
        if spec.model is not None:
            return spec.model.table
    return None


def _alias_entries(registry: OpsSpecRegistry) -> list[tuple[str, str]]:
    entries: list[tuple[str, str]] = []
    metrics = _pick_metric_table(registry)
    class_metrics = _pick_class_metric_table(registry)
    training = _pick_training_table(registry)
    model = _pick_model_table(registry)

    if metrics:
        entries.append(("metrics_on_subset", metrics))
        entries.append(("predictions", metrics))
    if class_metrics:
        entries.append(("metrics_by_class", class_metrics))
    if training:
        entries.append(("training_runs", training))
    if model:
        entries.append(("artifacts", model))
    elif training:
        entries.append(("artifacts", training))
    return entries


def _owning_spec(registry: OpsSpecRegistry, table: str) -> DatapipeOpsSpec | None:
    for spec in registry.list():
        if any(metric.table == table for metric in spec.metrics):
            return spec
        if any(metric.table == table for metric in spec.class_metrics):
            return spec
        if spec.training is not None and spec.training.status_table == table:
            return spec
        if spec.model is not None and spec.model.table == table:
            return spec
    return None


def _table_columns(
    registry: OpsSpecRegistry,
    *,
    table: str,
    ds: DataStore | None,
    catalog: Catalog | None,
) -> list[dict[str, str]]:
    spec = _owning_spec(registry, table)
    if spec is None:
        return []
    try:
        schema = registry._schema_for(spec, table, catalog, ds)
    except Exception:
        return []
    return [{"name": column, "type": "unknown"} for column in sorted(schema.columns)]


def _table_row_count(
    engine: Any,
    *,
    table: str,
    catalog: Catalog | None,
) -> int:
    if catalog is None or table not in catalog.catalog:
        return 0
    store = catalog.catalog[table].store
    data_table = getattr(store, "data_table", None)
    if data_table is None:
        return 0
    with engine.connect() as conn:
        return int(conn.execute(select(func.count()).select_from(data_table)).scalar() or 0)


def build_sql_analytics_context(
    registry: OpsSpecRegistry,
    *,
    schema: str | None = None,
    ds: DataStore | None = None,
    catalog: Catalog | None = None,
    engine: Any = None,
) -> SqlAnalyticsContext:
    qualified: dict[str, str] = {}
    bare: dict[str, str] = {}
    schema_tables: list[dict[str, Any]] = []

    for alias, physical in _alias_entries(registry):
        qphysical = _qualified_table(physical, schema)
        qualified[f"{DATASOURCE}.{alias}"] = qphysical
        bare[alias] = qphysical

        row_count = 0
        if engine is not None:
            try:
                row_count = _table_row_count(engine, table=physical, catalog=catalog)
            except Exception:
                row_count = 0

        schema_tables.append(
            {
                "name": alias,
                "physical_table": physical,
                "columns": _table_columns(registry, table=physical, ds=ds, catalog=catalog),
                "row_count": row_count,
            }
        )

    return SqlAnalyticsContext(
        qualified_table_map=qualified,
        bare_table_map=bare,
        schema_tables=schema_tables,
    )


def get_sql_schema_response(ctx: SqlAnalyticsContext) -> dict[str, Any]:
    return {
        "datasource": DATASOURCE,
        "tables": [
            {
                "name": table["name"],
                "columns": table["columns"],
                "row_count": table["row_count"],
                "physical_table": table.get("physical_table"),
            }
            for table in ctx.schema_tables
        ],
    }
