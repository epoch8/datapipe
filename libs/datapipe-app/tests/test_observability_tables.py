import pytest

from datapipe.compute import Catalog, Table
from datapipe.store.database import DBConn, TableStoreDB
from datapipe_app.observability.tables import (
    ObservabilityTableConfig,
    validate_observability_tables_against_catalog,
)
from sqlalchemy import Column, Integer


def test_observability_table_config_rejects_duplicate_names() -> None:
    with pytest.raises(ValueError, match="Duplicate datapipe-app observability table name 'same'"):
        ObservabilityTableConfig(
            pipeline_runs="same",
            pipeline_run_logs="same",
        )


def test_validate_observability_tables_against_catalog_rejects_pipeline_overlap() -> None:
    dbconn = DBConn("sqlite:///:memory:")
    catalog = Catalog(
        {
            "pipeline_runs": Table(
                store=TableStoreDB(
                    name="pipeline_runs",
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
