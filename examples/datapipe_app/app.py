import os
from typing import Tuple

import pandas as pd
from datapipe.compute import Catalog, DataStore, Pipeline, Table
from datapipe.step.batch_transform import BatchTransform
from datapipe.store.database import DBConn, TableStoreDB
from sqlalchemy import JSON, Boolean, Column, Integer, String

from datapipe_app import DatapipeAPI

DB_CONN_URI = os.environ.get("DB_CONN_URI", "sqlite+pysqlite3:///store.sqlite")

# dbconn = DBConn("sqlite:///store.sqlite")
# dbconn = DBConn("sqlite:///:memory:")
# dbconn = DBConn("postgresql://postgres:postgres@localhost:5432/postgres")
dbconn = DBConn(DB_CONN_URI)

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
            inputs=["events"],
            outputs=["user_profile", "user_lang"],
        ),
    ]
)

ds = DataStore(dbconn, create_meta_table=False)

app = DatapipeAPI(ds, catalog, pipeline)
