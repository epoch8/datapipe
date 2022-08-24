import pandas as pd
from sqlalchemy.sql.expression import select

from datapipe.run_config import RunConfig
from datapipe.store.database import TableStoreDB
from datapipe.datatable import DataStore
from datapipe.compute import Catalog, Pipeline,\
    Table
from datapipe.core_steps import BatchTransform, BatchGenerate
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


def test_meta_info_in_datapipe_events(dbconn) -> None:
    ds = DataStore(dbconn, create_meta_table=True)

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
                TEST_SCHEMA,
                create_table=True,
            )
        ),
        'test_transform': Table(
            store=TableStoreDB(
                dbconn,
                'test_transform_data',
                TEST_SCHEMA,
                create_table=True,
            )
        )
    })

    pipeline = Pipeline([
        BatchGenerate(
            generate_data,
            outputs=["test_generate"],
        ),
        BatchTransform(
            update_data,
            inputs=["test_generate"],
            outputs=["test_transform"],
        )
    ])

    run_pipeline(ds, catalog, pipeline, run_config)

    df_events = pd.read_sql_query(select(catalog.get_datatable(ds, 'test_generate').event_logger.events_table), dbconn.con)

    assert df_events.loc[0]["event"] == {
        "meta": {
            "labels": {
                "step_name": "generate_data",
                "pipeline_name": "test_name",
                "pipeline_id": 1,
            },
            "filters": {
                "pipeline_id": 1,
            }
        },
        "data": {
            "table_name": "test_generate",
            "added_count": 1,
            "updated_count": 0,
            "deleted_count": 0,
            "processed_count": 1
        }
    }

    assert df_events.loc[1]["event"] == {
        "meta": {
            "labels": {
                "step_name": "update_data",
                "pipeline_name": "test_name",
                "pipeline_id": 1,
            },
            "filters": {
                "pipeline_id": 1,
            }
        },
        "data": {
            "table_name": "test_transform",
            "added_count": 1,
            "updated_count": 0,
            "deleted_count": 0,
            "processed_count": 1
        }
    }
