from __future__ import annotations

from typing import TYPE_CHECKING, cast

from sqlalchemy import MetaData, Table
from sqlalchemy.orm import DeclarativeBase

from datapipe_app.observability.tables import ObservabilityTableConfig

if TYPE_CHECKING:
    from datapipe.compute import DatapipeApp
    from datapipe.store.database import DBConn
    from datapipe_app.datapipe_api import DatapipeAPI


metadata = MetaData()


class ObservabilityBase(DeclarativeBase):
    metadata = metadata


def apply_observability_table_config(
    tables: ObservabilityTableConfig,
    schema: str | None,
) -> None:
    from datapipe_app.observability.db import (
        PipelineMetricsCandidateRow,
        PipelineRegistryRow,
        PipelineRunLogRow,
        PipelineRunRow,
        PipelineRunStepRow,
        PipelineScheduleRow,
    )

    mapping: dict[type[ObservabilityBase], str] = {
        PipelineRegistryRow: tables.pipeline_registry,
        PipelineRunRow: tables.pipeline_runs,
        PipelineRunStepRow: tables.pipeline_run_steps,
        PipelineRunLogRow: tables.pipeline_run_logs,
        PipelineScheduleRow: tables.pipeline_schedules,
        PipelineMetricsCandidateRow: tables.metrics_candidates,
    }
    for model_cls, table_name in mapping.items():
        table = cast(Table, model_cls.__table__)
        table.name = table_name
        table.schema = schema

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
        PipelineMetricsCandidateRow,
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
        PipelineMetricsCandidateRow,
    ):
        src = cast(Table, model_cls.__table__)
        if not _metadata_has_table(target, src.name, dbconn.schema):
            if dbconn.schema is not None:
                src.to_metadata(target, schema=dbconn.schema)
            else:
                src.to_metadata(target)

    for src_table in analytics_metadata().tables.values():
        if not _metadata_has_table(target, src_table.name, dbconn.schema):
            if dbconn.schema is not None:
                src_table.to_metadata(target, schema=dbconn.schema)
            else:
                src_table.to_metadata(target)


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

    from datapipe_app.datapipe_api import DatapipeAPI

    tables = app.observability_table_config if isinstance(app, DatapipeAPI) and app.observability_table_config else ObservabilityTableConfig()

    if not _observability_tables_registered(dbconn, tables):
        if app.catalog is not None and app.catalog.catalog:
            from datapipe_app.observability.tables import validate_observability_tables_against_catalog

            validate_observability_tables_against_catalog(tables, app.catalog)

        if dbconn.schema is not None and not dbconn.connstr.startswith("sqlite"):
            from datapipe.store.database import ensure_db_schema

            ensure_db_schema(dbconn)
        register_observability_tables_in_metadata(dbconn, tables=tables)
