from re import I
import pandas as pd
from sqlalchemy import Column
from sqlalchemy.sql.sqltypes import Integer, String
from datapipe.compute import run_pipeline
from datapipe.core_steps import UpdateExternalTable
from datapipe.datatable import DataStore
from datapipe.store.database import DBConn, TableStoreDB
from datapipe.compute import Table, Catalog, Pipeline
from datapipe.run_config import RunConfig
from .util import assert_df_equal


def test_external_table_updater_filter(dbconn: DBConn):
    test_store = TableStoreDB(
        dbconn=dbconn,
        name="test_data",
        data_sql_schema=[
            Column("composite_id_1", Integer(), primary_key=True),
            Column("composite_id_2", Integer(), primary_key=True),
            Column("data", String())
        ]
    )
    df_test = pd.DataFrame({
        "composite_id_1": [1, 1, 2, 2],
        "composite_id_2": [3, 4, 5, 6],
        "data": ['a', 'b', 'c', 'd']
    })

    catalog = Catalog({
        "test": Table(store=test_store),
    })
    pipeline = Pipeline([
        UpdateExternalTable(
            output="test"
        )
    ])
    ds = DataStore(dbconn)

    test_store.insert_rows(df_test)

    run_pipeline(ds, catalog, pipeline)
    assert_df_equal(catalog.get_datatable(ds, "test").get_data(),
                    df_test,
                    index_cols=["composite_id_1", "composite_id_2"])

    config = RunConfig(filters={"composite_id_1": 2})
    run_pipeline(ds, catalog, pipeline, run_config=config)
    assert_df_equal(catalog.get_datatable(ds, "test").get_data(),
                    df_test,
                    index_cols=["composite_id_1", "composite_id_2"])
