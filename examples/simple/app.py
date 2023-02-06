from typing import Iterator

from pathlib import Path

import pandas as pd

from sqlalchemy import Column, String

from datapipe.compute import Catalog, Pipeline, Table
from datapipe.core_steps import BatchTransform, UpdateExternalTable
from datapipe.datatable import DataStore
from datapipe.store.database import DBConn, TableStoreDB

from datapipe_app import DatapipeApp

from datapipe.core_steps import do_full_batch_transform, BatchGenerate
import ray
import time
import uuid

dbconn = DBConn("postgresql://postgres:password@localhost/postgres")

catalog = Catalog(
    {
        "input": Table(
            store=TableStoreDB(
                name="input",
                dbconn=dbconn,
                data_sql_schema=[
                    Column("id", String, primary_key=True),
                    Column("a", String),
                ],
            ),
        ),
        "output": Table(
            store=TableStoreDB(
                name="output",
                dbconn=dbconn,
                data_sql_schema=[
                    Column("id", String, primary_key=True),
                    Column("a", String),
                ],
            )
        ),
    }
)


def gen_some_data() -> Iterator[pd.DataFrame]:
    yield pd.DataFrame(
        {
            "id": [f"id_{i}" for i in range(100)],
            "a": [str(uuid.uuid4()) for i in range(100)],
        }
    )


def simple_func(df: pd.DataFrame) -> pd.DataFrame:
    time.sleep(10)
    return df


pipeline = Pipeline(
    [
        BatchGenerate(
            gen_some_data,
            outputs=["input"],
        ),
        BatchTransform(
            simple_func,
            inputs=["input"],
            outputs=["output"],
            chunk_size=10,
        ),
    ]
)


ds = DataStore(dbconn)

app = DatapipeApp(ds, catalog, pipeline)
