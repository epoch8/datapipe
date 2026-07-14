import tempfile

import pandas as pd
import pytest
from datapipe.compute import Catalog, DataStore, Pipeline, Table
from datapipe.step.batch_transform import BatchTransform
from datapipe.store.database import DBConn, TableStoreDB
from fastapi.testclient import TestClient
from sqlalchemy import Column, Integer, String

from datapipe_app import DatapipeAPI


@pytest.fixture
def agent_env(monkeypatch):
    monkeypatch.delenv("DATAPIPE_APP_PIPELINE_ID", raising=False)


@pytest.fixture
def ops_app(agent_env):
    with tempfile.TemporaryDirectory() as tmpdir:
        dbconn = DBConn(f"sqlite+pysqlite3:///{tmpdir}/store.sqlite")

        def transform(df: pd.DataFrame) -> pd.DataFrame:
            return df.assign(v=df["v"].astype(str) + "_x")

        catalog = Catalog(
            {
                "input": Table(
                    store=TableStoreDB(
                        name="input",
                        dbconn=dbconn,
                        data_sql_schema=[
                            Column("id", Integer(), primary_key=True),
                            Column("v", String()),
                        ],
                        create_table=True,
                    )
                ),
                "output": Table(
                    store=TableStoreDB(
                        name="output",
                        dbconn=dbconn,
                        data_sql_schema=[
                            Column("id", Integer(), primary_key=True),
                            Column("v", String()),
                        ],
                        create_table=True,
                    )
                ),
            }
        )

        pipeline = Pipeline(
            [
                BatchTransform(
                    transform,
                    inputs=["input"],
                    outputs=["output"],
                    labels=[("stage", "process")],
                ),
            ]
        )
        ds = DataStore(dbconn, create_meta_table=True)
        input_dt = catalog.get_datatable(ds, "input")
        catalog.get_datatable(ds, "output")
        input_dt.store_chunk(pd.DataFrame([{"id": 1, "v": "a"}, {"id": 2, "v": "b"}]))
        yield DatapipeAPI(ds, catalog, pipeline, pipeline_id="test_pipeline")


@pytest.fixture
def ops_client(ops_app):
    return TestClient(ops_app)
