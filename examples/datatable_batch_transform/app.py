from typing import Dict, List, Optional

import numpy as np
import pandas as pd
from sqlalchemy import Integer
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.sql import functions, select
from sqlalchemy.sql.schema import Column

from datapipe.compute import Catalog, DatapipeApp, Pipeline, Table
from datapipe.datatable import DataStore, DataTable
from datapipe.run_config import RunConfig
from datapipe.step.batch_generate import BatchGenerate
from datapipe.step.batch_transform import DatatableBatchTransform
from datapipe.store.database import DBConn, TableStoreDB
from datapipe.types import IndexDF


class Base(DeclarativeBase):
    pass


class Input(Base):
    __tablename__ = "input"

    group_id: Mapped[int] = mapped_column(primary_key=True)
    item_id: Mapped[int] = mapped_column(primary_key=True)


class Output(Base):
    __tablename__ = "output"

    group_id: Mapped[int] = mapped_column(primary_key=True)
    count: Mapped[int]


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
    kwargs: Optional[Dict] = None,
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

    with ds.meta_dbconn.con.begin() as con:
        return pd.read_sql_query(
            sql,
            con=con,
        )


pipeline = Pipeline(
    [
        BatchGenerate(
            generate_data,
            outputs=[Input],
        ),
        DatatableBatchTransform(
            count_tbl,
            inputs=[Input],
            outputs=[Output],
        ),
    ]
)


dbconn = DBConn("sqlite+pysqlite3:///db.sqlite", sqla_metadata=Base.metadata)
ds = DataStore(dbconn)

app = DatapipeApp(ds=ds, catalog=Catalog({}), pipeline=pipeline)
