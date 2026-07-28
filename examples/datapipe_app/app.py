import os
from typing import Tuple

import pandas as pd
from datapipe.compute import Catalog, Pipeline, Table
from datapipe.datatable import DataStore
from datapipe.step.batch_transform import BatchTransform
from datapipe.store.database import DBConn, TableStoreDB
from datapipe_app import DatapipeAPI
from sqlalchemy import JSON, Boolean, Column, Integer, String

DB_CONN_URI = os.environ.get("DB_CONN_URI", "sqlite+pysqlite3:///store.sqlite")

# dbconn = DBConn("sqlite:///store.sqlite")
# dbconn = DBConn("sqlite:///:memory:")
# dbconn = DBConn("postgresql+psycopg://postgres:postgres@localhost:5432/postgres")
dbconn = DBConn(DB_CONN_URI)

events_tbl = Table(
    name="events",
    store=TableStoreDB(
        name="events",
        dbconn=dbconn,
        data_sql_schema=[
            Column("user_id", Integer(), primary_key=True),
            Column("event_id", Integer(), primary_key=True),
            Column("event", JSON()),
        ],
        create_table=False,
    ),
)

user_profile_tbl = Table(
    name="user_profile",
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
    ),
)

user_lang_tbl = Table(
    name="user_lang",
    store=TableStoreDB(
        name="user_lang",
        dbconn=dbconn,
        data_sql_schema=[
            Column("user_id", Integer(), primary_key=True),
            Column("lang", String(length=100)),
        ],
        create_table=False,
    ),
)


def agg_profile(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    res = []

    res_lang = []

    for user_id, grp in df.groupby("user_id"):
        res.append(
            {
                "user_id": user_id,
                "offer_clicks": [x["offer_id"] for x in grp["event"] if x["event_type"] == "click"],
                "events_count": len(grp),
                "active": True,
            }
        )

        res_lang.append(
            {
                "user_id": user_id,
                "lang": grp.iloc[-1]["event"]["lang"],
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
            inputs=[events_tbl],
            outputs=[user_profile_tbl, user_lang_tbl],
        ),
    ]
)

ds = DataStore(dbconn, create_meta_table=False)

app = DatapipeAPI(ds, Catalog({}), pipeline)
