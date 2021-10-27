import json

import pandas as pd

from datapipe.step import RunConfig
from datapipe.store.database import TableStoreDB
from datapipe.datatable import DataStore
from datapipe.dsl import Catalog, Pipeline, BatchTransform, BatchGenerate,\
    Table
from datapipe.compute import run_pipeline

from sqlalchemy.sql.schema import Column
from sqlalchemy.sql.sqltypes import Integer, JSON


TEST_SCHEMA = [
    Column('pipeline_id', Integer(), primary_key=True),
    Column('offer_id', Integer(), primary_key=True),
    Column('test_field', JSON)
]


def generate_data():
    df_data = [{
        "pipeline_id": 1,
        "offer_id": 1,
        "test_field": {"a": 1}
    }]
    yield pd.DataFrame(data=df_data)


def update_data(df: pd.DataFrame) -> pd.DataFrame:
    df["test_field"].apply(lambda x: {**x, "b": 2})
    df.index = df.index.astype('str')
    return df


def test_full_run_config(dbconn) -> None:
    ds = DataStore(dbconn)

    run_config = RunConfig(
        filters={
            "pipeline_id": 1
        },
        labels={
            "pipeline_name": 'test_name',
            "pipeline_id": 1
        }
    )

    catalog = Catalog({
        'test_generate': Table(
            store=TableStoreDB(
                dbconn,
                'test_generate_data',
                TEST_SCHEMA
            )
        ),
        'test_transform': Table(
            store=TableStoreDB(
                dbconn,
                'test_transform_data',
                TEST_SCHEMA
            )
        )
    })

    pipeline = Pipeline([
        BatchGenerate(
            generate_data,
            outputs=["test_generate"]
        ),
        BatchTransform(
            update_data,
            inputs=["test_generate"],
            outputs=["test_transform"],
        )
    ])

    run_pipeline(ds, catalog, pipeline, run_config)

    df_events = pd.read_sql_query("select * from public.datapipe_events", dbconn.con)

    assert json.loads(df_events.loc[0]["event"]) == {
        "meta": {
            "step_name": "generate_data",
            "pipeline_name": "test_name",
            "pipeline_id": 1,
            "filters": {
                "pipeline_id": 1
            }
        },
        "data": {
            "table_name": "test_generate",
            "added_count": 1,
            "updated_count": 0,
            "deleted_count": 0
        }
    }

    assert json.loads(df_events.loc[1]["event"]) == {
        "meta": {
            "step_name": "update_data",
            "pipeline_name": "test_name",
            "pipeline_id": 1,
            "filters": {
                "pipeline_id": 1
            }
        },
        "data": {
            "table_name": "test_transform",
            "added_count": 1,
            "updated_count": 0,
            "deleted_count": 0
        }
    }
