from datapipe.store.database import DBConn
from sqlalchemy import Column, Integer, inspect

from datapipe_app.db_schema import (
    create_observability_tables_hook,
    register_observability_tables_in_metadata,
)
from datapipe_app.observability.tables import ObservabilityTableConfig


def test_register_observability_tables_in_metadata() -> None:
    dbconn = DBConn("sqlite:///:memory:")
    register_observability_tables_in_metadata(dbconn)

    table_names = {table.name for table in dbconn.sqla_metadata.tables.values()}
    config = ObservabilityTableConfig()
    assert config.pipeline_runs in table_names
    assert config.pipeline_run_logs in table_names
    assert config.analytics_metrics_on_subset in table_names


def test_create_observability_tables_hook_creates_tables(ops_app) -> None:
    dbconn = ops_app.ds.meta_dbconn
    inspector = inspect(dbconn.con)

    create_observability_tables_hook(ops_app, dbconn)
    dbconn.sqla_metadata.create_all(dbconn.con)
    create_observability_tables_hook(ops_app, dbconn)

    after = set(inspector.get_table_names())
    assert "pipeline_runs" in after
    assert "pipeline_run_logs" in after
    assert "analytics_metrics_on_subset" in after
