from datapipe.store.database import DBConn

from datapipe_app.db_schema import register_observability_tables_in_metadata
from datapipe_app.observability.tables import ObservabilityTableConfig
from datapipe_app_ml_ops.observability.tables import MLObservabilityTableConfig


def test_register_observability_tables_in_metadata() -> None:
    dbconn = DBConn("sqlite:///:memory:")
    register_observability_tables_in_metadata(dbconn)

    table_names = {table.name for table in dbconn.sqla_metadata.tables.values()}
    config = ObservabilityTableConfig()
    ml_config = MLObservabilityTableConfig()
    assert config.pipeline_runs in table_names
    assert config.pipeline_run_logs in table_names
    assert ml_config.metrics_candidates in table_names
    assert ml_config.analytics_metrics_on_subset in table_names
