from datapipe.compute import DataStore, Table, Catalog, Pipeline, run_pipeline
from datapipe.core_steps import BatchGenerate, BatchTransform
from datapipe.store.database import TableStoreDB
from sqlalchemy import Column
from sqlalchemy.sql.sqltypes import Integer
import pandas as pd


def _build_gen_func():
    counter = 0

    def func():
        nonlocal counter
        if counter == 0:
            yield pd.DataFrame({
                "id": [1, 2]
            })
        elif counter == 1:
            yield pd.DataFrame({
                "id": [2]
            })
        else:
            raise ValueError()
        counter += 1

    return func


def test_batchgenerate_removal(dbconn):
    ds = DataStore(dbconn)
    catalog = Catalog({
        "input": Table(
            store=TableStoreDB(
                dbconn,
                "input_data",
                data_sql_schema=[
                    Column("id", Integer(), primary_key=True),
                ]
            )
        ),
        "output": Table(
            store=TableStoreDB(
                dbconn,
                "output_data",
                data_sql_schema=[
                    Column("id", Integer(), primary_key=True),
                ]
            )
        ),
    })
    pipeline = Pipeline([
        BatchGenerate(
            _build_gen_func(),
            outputs=["input"]
        ),
        BatchTransform(
            lambda df: df,
            inputs=["input"],
            outputs=["output"]
        )
    ])
    run_pipeline(ds, catalog, pipeline)
    assert catalog.get_datatable(ds, "input").get_data().shape[0] == 2
    assert catalog.get_datatable(ds, "output").get_data().shape[0] == 2
    run_pipeline(ds, catalog, pipeline)
    assert catalog.get_datatable(ds, "input").get_data().shape[0] == 1
    assert catalog.get_datatable(ds, "output").get_data().shape[0] == 1
