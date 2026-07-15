from __future__ import annotations

from typing import Any, cast

from sqlalchemy import MetaData, Table

from datapipe_app.observability.config.tables import ObservabilityTableConfig
from datapipe_app_ml_ops.observability.config.tables import MLObservabilityTableConfig
from datapipe_app_ml_ops.observability.store.db_models import (
    PipelineMetricsCandidateRow,
    apply_ml_table_config,
)


def extend_observability_db(
    *,
    phase: str,
    tables: ObservabilityTableConfig | None = None,
    schema: str | None = None,
    dbconn: Any = None,
    include_run_logs: bool = True,
    metadata: MetaData | None = None,
) -> None:
    ml_tables = MLObservabilityTableConfig()
    if phase == "apply_config":
        apply_ml_table_config(tables=ml_tables, schema=schema)
        return

    if phase != "register_metadata" or metadata is None or dbconn is None:
        return

    apply_ml_table_config(tables=ml_tables, schema=dbconn.schema)
    src = cast(Table, PipelineMetricsCandidateRow.__table__)
    if not _metadata_has_table(metadata, src.name, dbconn.schema):
        if dbconn.schema is not None:
            src.to_metadata(metadata, schema=dbconn.schema)
        else:
            src.to_metadata(metadata)


def _metadata_has_table(metadata: MetaData, name: str, schema: str | None) -> bool:
    for table in metadata.tables.values():
        if table.name == name and table.schema == schema:
            return True
    return False
