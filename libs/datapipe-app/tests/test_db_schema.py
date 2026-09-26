from sqlalchemy import inspect

from datapipe_app.app.db_schema import create_observability_tables_hook


from datapipe_app.observability.config.tables import ObservabilityTableConfig


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
