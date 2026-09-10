from sqlalchemy import inspect
from unittest.mock import MagicMock

from datapipe_app.app.db_schema import (
    _ensure_clickhouse_run_logs_table,
    create_observability_tables_hook,
)
from datapipe_app.observability.config.tables import ObservabilityTableConfig
from datapipe_app.observability.run_logs.store import ClickHouseRunLogStore


def test_create_observability_tables_hook_creates_tables(ops_app) -> None:
    dbconn = ops_app.ds.meta_dbconn
    inspector = inspect(dbconn.con)
    tables = ObservabilityTableConfig()

    create_observability_tables_hook(ops_app, dbconn)
    dbconn.sqla_metadata.create_all(dbconn.con)
    create_observability_tables_hook(ops_app, dbconn)

    after = set(inspector.get_table_names())
    assert tables.pipeline_runs in after
    assert tables.pipeline_run_steps in after
    assert "datapipe_api__run_logs" not in after


def test_ensure_clickhouse_run_logs_table_calls_ensure_table() -> None:
    store = MagicMock(spec=ClickHouseRunLogStore)
    backend = MagicMock()
    backend.store = store
    app = MagicMock()
    app.run_logs_backend = backend

    _ensure_clickhouse_run_logs_table(app)
    store.ensure_table.assert_called_once_with()


def test_ensure_clickhouse_run_logs_table_skips_non_clickhouse() -> None:
    backend = MagicMock()
    backend.store = object()
    app = MagicMock()
    app.run_logs_backend = backend

    _ensure_clickhouse_run_logs_table(app)
