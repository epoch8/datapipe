from sqlalchemy import inspect

from datapipe_app.db_schema import create_observability_tables_hook


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
