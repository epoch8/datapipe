import pandas as pd
from datapipe.datatable import DataTable, DBConn
from datapipe.compute import Pipeline, Catalog, DataStore, Table, run_pipeline
from datapipe.core_steps import BatchTransform
from datapipe.store.database import TableStoreDB
from sqlalchemy import Column
from sqlalchemy.sql.sqltypes import Integer


SCHEMA_INPUT_1 = [
    Column("input_1_id", Integer(), primary_key=True),
]
SCHEMA_INPUT_2 = [
    Column("input_2_id", Integer(), primary_key=True),
]
SCHEMA_OUTPUT = [
    Column("input_1_id", Integer(), primary_key=True),
    Column("input_2_id", Integer(), primary_key=True)
]


def test_non_inersection_inputs_inersect_output(dbconn: DBConn):
    ds = DataStore(dbconn)
    catalog = Catalog({
        "input_1": Table(
            store=TableStoreDB(
                dbconn,
                name="input_1_data",
                data_sql_schema=SCHEMA_INPUT_1,
            ),
        ),
        "input_2": Table(
            store=TableStoreDB(
                dbconn,
                name="input_2_data",
                data_sql_schema=SCHEMA_INPUT_2,
            ),
        ),
        "output": Table(
            store=TableStoreDB(
                dbconn,
                name="output_data",
                data_sql_schema=SCHEMA_OUTPUT,
            )
        )
    })
    dt_input_1 = catalog.get_datatable(ds, "input_1")
    dt_input_2 = catalog.get_datatable(ds, "input_2")
    dt_output = catalog.get_datatable(ds, "output")
    dt_input_1.store_chunk(
        pd.DataFrame({
            "input_1_id": [3, 4, 5],
        })
    )
    dt_input_2.store_chunk(
        pd.DataFrame({
            "input_2_id": [6, 7, 8, 9, 10],
        })
    )
    dt_output.store_chunk(
        pd.DataFrame({
            "input_1_id": [1, 2, 3, 4, 5],
            "input_2_id": [6, 7, 8, 9, 10],
        })
    )
    dt_input_1.store_chunk(
        pd.DataFrame({
            "input_1_id": [1, 2],
        })
    )

    idx_count, idx_gen = ds.get_process_ids(
        inputs=[dt_input_1, dt_input_2],
        outputs=[dt_output],
        chunksize=1000,
        run_config=None
    )
    assert idx_count == 2
