import pytest

from datapipe.compute import Catalog, Table
from datapipe.store.database import DBConn, TableStoreDB
from datapipe_app.observability.config.tables import (
    ObservabilityTableConfig,
    ensure_observability_tables_compatible_with_pipeline,
    validate_observability_tables_against_catalog,
)
from sqlalchemy import Column, Integer

OBS_RUNS = ObservabilityTableConfig().pipeline_runs


def test_observability_table_config_rejects_duplicate_names() -> None:
    with pytest.raises(ValueError, match="Duplicate datapipe-app observability table name 'same'"):
        ObservabilityTableConfig(
            pipeline_runs="same",
            pipeline_run_steps="same",
        )


def test_validate_observability_tables_against_catalog_rejects_pipeline_overlap() -> None:
    dbconn = DBConn("sqlite:///:memory:")
    catalog = Catalog(
        {
            OBS_RUNS: Table(
                store=TableStoreDB(
                    name=OBS_RUNS,
                    dbconn=dbconn,
                    data_sql_schema=[Column("id", Integer, primary_key=True)],
                    create_table=False,
                )
            ),
        }
    )
    config = ObservabilityTableConfig()

    with pytest.raises(ValueError, match="must not reuse pipeline catalog table names"):
        validate_observability_tables_against_catalog(config, catalog)


def test_ensure_observability_tables_compatible_skips_different_dbconn() -> None:
    pipeline_dbconn = DBConn("sqlite:///:memory:", "pipeline_schema")
    observability_dbconn = DBConn("sqlite:///:memory:", "observability_schema")
    catalog = Catalog(
        {
            OBS_RUNS: Table(
                store=TableStoreDB(
                    name=OBS_RUNS,
                    dbconn=pipeline_dbconn,
                    data_sql_schema=[Column("id", Integer, primary_key=True)],
                    create_table=False,
                )
            ),
        }
    )
    config = ObservabilityTableConfig()

    ensure_observability_tables_compatible_with_pipeline(
        observability_dbconn=observability_dbconn,
        pipeline_dbconn=pipeline_dbconn,
        config=config,
        catalog=catalog,
    )


def test_ensure_observability_tables_compatible_rejects_overlap_on_shared_dbconn() -> None:
    dbconn = DBConn("sqlite:///:memory:", "shared_schema")
    catalog = Catalog(
        {
            OBS_RUNS: Table(
                store=TableStoreDB(
                    name=OBS_RUNS,
                    dbconn=dbconn,
                    data_sql_schema=[Column("id", Integer, primary_key=True)],
                    create_table=False,
                )
            ),
        }
    )
    config = ObservabilityTableConfig()

    with pytest.raises(ValueError, match="must not reuse pipeline catalog table names"):
        ensure_observability_tables_compatible_with_pipeline(
            observability_dbconn=dbconn,
            pipeline_dbconn=dbconn,
            config=config,
            catalog=catalog,
        )
