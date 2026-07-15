from __future__ import annotations

from typing import TYPE_CHECKING, cast

from sqlalchemy import MetaData, Table
from sqlalchemy.orm import DeclarativeBase

from datapipe_app.observability.config.tables import ObservabilityTableConfig
from datapipe_app.observability.extensions import run_observability_db_extensions

if TYPE_CHECKING:
    from datapipe.compute import DatapipeApp
    from datapipe.store.database import DBConn
    from datapipe_app.app.datapipe_api import DatapipeAPI

metadata = MetaData()


class ObservabilityBase(DeclarativeBase):
    metadata = metadata


def apply_observability_table_config(
    tables: ObservabilityTableConfig,
    schema: str | None,
) -> None:
    from datapipe_app.observability.store.db import (
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
    }
    for model_cls, table_name in mapping.items():
        table = cast(Table, model_cls.__table__)
        table.name = table_name
        table.schema = schema

    run_observability_db_extensions(phase="apply_config", tables=tables, schema=schema)


def register_observability_tables_in_metadata(
    dbconn: DBConn,
    *,
    tables: ObservabilityTableConfig | None = None,
    include_run_logs: bool = True,
) -> None:
    """Attach datapipe-app observability tables to the pipeline ``sqla_metadata``."""
    tables = tables or ObservabilityTableConfig()
    apply_observability_table_config(tables, dbconn.schema)

    from datapipe_app.observability.store.db import (
        PipelineRegistryRow,
        PipelineRunLogRow,
        PipelineRunRow,
        PipelineRunStepRow,
        PipelineScheduleRow,
    )

    target = dbconn.sqla_metadata
    model_classes = [
        PipelineRegistryRow,
        PipelineRunRow,
        PipelineRunStepRow,
        PipelineScheduleRow,
    ]
    if include_run_logs:
        model_classes.insert(3, PipelineRunLogRow)
    for model_cls in model_classes:
        src = cast(Table, model_cls.__table__)
        if not _metadata_has_table(target, src.name, dbconn.schema):
            if dbconn.schema is not None:
                src.to_metadata(target, schema=dbconn.schema)
            else:
                src.to_metadata(target)

    run_observability_db_extensions(
        phase="register_metadata",
        dbconn=dbconn,
        tables=tables,
        include_run_logs=include_run_logs,
        metadata=target,
    )


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

    from datapipe_app.app.datapipe_api import DatapipeAPI

    tables = app.observability_table_config if isinstance(app, DatapipeAPI) and app.observability_table_config else ObservabilityTableConfig()

    include_run_logs = True
    if isinstance(app, DatapipeAPI):
        include_run_logs = app.run_logs_backend is None

    if not _observability_tables_registered(dbconn, tables):
        if app.catalog is not None and app.catalog.catalog:
            from datapipe_app.observability.config.tables import (
                ensure_observability_tables_compatible_with_pipeline,
            )

            observability_dbconn = (
                app.observability_dbconn
                if isinstance(app, DatapipeAPI)
                else dbconn
            )
            ensure_observability_tables_compatible_with_pipeline(
                observability_dbconn=observability_dbconn,
                pipeline_dbconn=dbconn,
                config=tables,
                catalog=app.catalog,
            )

        if dbconn.schema is not None and not dbconn.connstr.startswith("sqlite"):
            from datapipe.store.database import ensure_db_schema

            ensure_db_schema(dbconn)
        register_observability_tables_in_metadata(
            dbconn,
            tables=tables,
            include_run_logs=include_run_logs,
        )
