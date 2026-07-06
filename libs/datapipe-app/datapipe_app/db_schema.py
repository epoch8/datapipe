from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import MetaData, text
from sqlalchemy.engine import Engine

from datapipe_app.observability.tables import ObservabilityTableConfig

if TYPE_CHECKING:
    from datapipe.compute import DatapipeApp
    from datapipe.store.database import DBConn


def apply_observability_table_config(
    tables: ObservabilityTableConfig,
    schema: str | None,
) -> None:
    from datapipe_app.observability.db import (
        PipelineRegistryRow,
        PipelineRunLogRow,
        PipelineRunRow,
        PipelineRunStepRow,
        PipelineScheduleRow,
    )

    mapping = {
        PipelineRegistryRow: tables.pipeline_registry,
        PipelineRunRow: tables.pipeline_runs,
        PipelineRunStepRow: tables.pipeline_run_steps,
        PipelineRunLogRow: tables.pipeline_run_logs,
        PipelineScheduleRow: tables.pipeline_schedules,
    }
    for model_cls, table_name in mapping.items():
        model_cls.__table__.name = table_name
        model_cls.__table__.schema = schema

    from datapipe_app.observability.analytics_views import apply_analytics_table_config

    apply_analytics_table_config(tables=tables, schema=schema)


def register_observability_tables_in_metadata(
    dbconn: DBConn,
    *,
    tables: ObservabilityTableConfig | None = None,
) -> None:
    """Attach datapipe-app observability tables to the pipeline ``sqla_metadata``."""
    tables = tables or ObservabilityTableConfig()
    apply_observability_table_config(tables, dbconn.schema)

    from datapipe_app.observability.analytics_views import analytics_metadata
    from datapipe_app.observability.db import (
        PipelineRegistryRow,
        PipelineRunLogRow,
        PipelineRunRow,
        PipelineRunStepRow,
        PipelineScheduleRow,
    )

    target = dbconn.sqla_metadata
    for model_cls in (
        PipelineRegistryRow,
        PipelineRunRow,
        PipelineRunStepRow,
        PipelineRunLogRow,
        PipelineScheduleRow,
    ):
        src = model_cls.__table__
        if not _metadata_has_table(target, src.name, dbconn.schema):
            src.to_metadata(target, schema=dbconn.schema)

    for src in analytics_metadata().tables.values():
        if not _metadata_has_table(target, src.name, dbconn.schema):
            src.to_metadata(target, schema=dbconn.schema)


def _metadata_has_table(metadata: MetaData, name: str, schema: str | None) -> bool:
    for table in metadata.tables.values():
        if table.name == name and table.schema == schema:
            return True
    return False


def _observability_tables_registered(dbconn: DBConn, tables: ObservabilityTableConfig) -> bool:
    return _metadata_has_table(dbconn.sqla_metadata, tables.pipeline_runs, dbconn.schema)


def create_observability_tables_hook(app: DatapipeApp, dbconn: DBConn) -> None:
    if app.ds is None:
        return

    tables = getattr(app, "observability_table_config", None) or ObservabilityTableConfig()

    if not _observability_tables_registered(dbconn, tables):
        if app.catalog is not None and app.catalog.catalog:
            from datapipe_app.observability.tables import validate_observability_tables_against_catalog

            validate_observability_tables_against_catalog(tables, app.catalog)

        if dbconn.schema is not None and not dbconn.connstr.startswith("sqlite"):
            from datapipe.store.database import ensure_db_schema

            ensure_db_schema(dbconn)
        register_observability_tables_in_metadata(dbconn, tables=tables)
        return

    _ensure_run_labels_column(dbconn.con, tables=tables, schema=dbconn.schema)


def _ensure_run_labels_column(
    engine: Engine,
    *,
    tables: ObservabilityTableConfig,
    schema: str | None,
) -> None:
    try:
        from sqlalchemy import inspect as sa_inspect

        inspector = sa_inspect(engine)
        runs_table = tables.pipeline_runs
        if runs_table not in inspector.get_table_names(schema=schema):
            return
        columns = {col["name"] for col in inspector.get_columns(runs_table, schema=schema)}
        if "labels_json" in columns:
            return
        qualified = f'"{schema}".{runs_table}' if schema else runs_table
        with engine.begin() as con:
            con.execute(text(f"ALTER TABLE {qualified} ADD COLUMN labels_json TEXT"))
    except Exception:
        return
