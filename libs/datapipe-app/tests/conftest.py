import tempfile
from typing import Tuple

import pandas as pd
import pytest
from datapipe.compute import Catalog, DataStore, Pipeline, Table
from datapipe.step.batch_transform import BatchTransform
from datapipe.store.database import DBConn, TableStoreDB
from fastapi.testclient import TestClient
from sqlalchemy import JSON, Boolean, Column, Integer, String

from datapipe_app import DatapipeAPI
from datapipe_app.observability.db import ObservabilityStore


@pytest.fixture
def observability_store():
    with tempfile.TemporaryDirectory() as tmpdir:
        url = f"sqlite+pysqlite3:///{tmpdir}/obs.sqlite"
        store = ObservabilityStore.from_url(url)
        yield store


@pytest.fixture
def agent_env(monkeypatch):
    monkeypatch.setenv("DATAPIPE_APP_MODE", "agent")
    monkeypatch.setenv("DATAPIPE_APP_PIPELINE_ID", "test_pipeline")


@pytest.fixture
def central_env(monkeypatch):
    monkeypatch.setenv("DATAPIPE_APP_MODE", "central")
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
        yield DatapipeAPI(ds, catalog, pipeline)


@pytest.fixture
def ops_client(ops_app):
    return TestClient(ops_app)


@pytest.fixture
def app():
    with tempfile.TemporaryDirectory() as tmpdir:
        dbconn = DBConn(f"sqlite+pysqlite3:///{tmpdir}/store.sqlite")

        catalog = Catalog(
            {
                "events": Table(
                    store=TableStoreDB(
                        name="events",
                        dbconn=dbconn,
                        data_sql_schema=[
                            Column("user_id", Integer(), primary_key=True),
                            Column("event_id", Integer(), primary_key=True),
                            Column("event", JSON()),
                        ],
                        create_table=False,
                    )
                ),
                "user_profile": Table(
                    store=TableStoreDB(
                        name="user_profile",
                        dbconn=dbconn,
                        data_sql_schema=[
                            Column("user_id", Integer(), primary_key=True),
                            Column("offer_clicks", JSON()),
                            Column("events_count", Integer()),
                            Column("active", Boolean()),
                        ],
                        create_table=False,
                    )
                ),
                "user_lang": Table(
                    store=TableStoreDB(
                        name="user_lang",
                        dbconn=dbconn,
                        data_sql_schema=[
                            Column("user_id", Integer(), primary_key=True),
                            Column("lang", String(length=100)),
                        ],
                        create_table=False,
                    )
                ),
            }
        )

        def agg_profile(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
            res = []
            res_lang = []
            for user_id, grp in df.groupby("user_id"):
                res.append(
                    {
                        "user_id": user_id,
                        "offer_clicks": [
                            x["offer_id"]
                            for x in grp["event"]
                            if x["event_type"] == "click"
                        ],
                        "events_count": len(grp),
                        "active": True,
                    }
                )
                res_lang.append(
                    {
                        "user_id": user_id,
                        "lang": grp.iloc[-1]["event"].get("lang"),
                    }
                )
            return (
                pd.DataFrame.from_records(res),
                pd.DataFrame.from_records(res_lang),
            )

        pipeline = Pipeline(
            steps=[
                BatchTransform(
                    agg_profile,
                    inputs=["events"],
                    outputs=["user_profile", "user_lang"],
                ),
            ]
        )

        ds = DataStore(dbconn, create_meta_table=False)
        api_app = DatapipeAPI(ds, catalog, pipeline)
        api_app.ds.meta_dbconn.sqla_metadata.create_all(api_app.ds.meta_dbconn.con)
        yield api_app
