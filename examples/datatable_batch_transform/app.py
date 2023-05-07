from typing import List, Optional

import numpy as np
import pandas as pd
from datapipe_app import DatapipeApp
from sqlalchemy import Integer
from sqlalchemy.sql import functions, select
from sqlalchemy.sql.schema import Column

from datapipe.compute import Catalog, Pipeline, Table
from datapipe.core_steps import BatchGenerate, DatatableBatchTransform
from datapipe.datatable import DataStore, DataTable
from datapipe.run_config import RunConfig
from datapipe.store.database import DBConn, TableStoreDB
from datapipe.types import IndexDF

dbconn = DBConn("sqlite+pysqlite3:///db.sqlite")


catalog = Catalog(
    {
        "input": Table(
            store=TableStoreDB(
                dbconn=dbconn,
                name="input",
                data_sql_schema=[
                    Column("group_id", Integer, primary_key=True),
                    Column("item_id", Integer, primary_key=True),
                ],
            )
        ),
        "result": Table(
            store=TableStoreDB(
                dbconn=dbconn,
                name="output",
                data_sql_schema=[
                    Column("group_id", Integer, primary_key=True),
                    Column("count", Integer),
                ],
            )
        ),
    }
)


def generate_data():
    # N = 10_000
    N = 100
    for i in range(10):
        print(i)
        yield pd.DataFrame(
            {
                "group_id": np.random.randint(0, 100, size=N),
                "item_id": np.random.randint(0, 100_000, size=N),
            }
        ).drop_duplicates()


def count(
    input_df: pd.DataFrame,
) -> pd.DataFrame:
    return input_df.groupby("group_id").agg(count=("item_id", np.size)).reset_index()


def count_tbl(
    ds: DataStore,
    idx: IndexDF,
    input_dts: List[DataTable],
    run_config: Optional[RunConfig] = None,
) -> pd.DataFrame:
    (input_dt,) = input_dts

    ts = input_dt.table_store
    assert isinstance(ts, TableStoreDB)

    tbl = ts.data_table
    sql = (
        select(
            tbl.c["group_id"],
            functions.count().label("count"),
        )
        .select_from(tbl)
        .where(tbl.c["group_id"].in_(idx["group_id"]))
        .group_by(tbl.c["group_id"])
    )

    return pd.read_sql_query(
        sql,
        con=ds.meta_dbconn.con,
    )


pipeline = Pipeline(
    [
        BatchGenerate(
            generate_data,
            outputs=["input"],
        ),
        DatatableBatchTransform(
            count_tbl,
            inputs=["input"],
            outputs=["result"],
        ),
    ]
)


ds = DataStore(dbconn)

app = DatapipeApp(ds, catalog, pipeline)
