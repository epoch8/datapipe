from datetime import date
from typing import Generator

import pandas as pd
import sqlalchemy as sa

from datapipe.compute import Catalog, DatapipeApp, Pipeline, Table
from datapipe.datatable import DataStore
from datapipe.step.batch_generate import BatchGenerate
from datapipe.step.chain_transform import ChainTransform, ChainTransformStep
from datapipe.store.database import DBConn, TableStoreDB
from datapipe.executor import ExecutorConfig
from datapipe.types import ChangeList

from examples.datapipe_core._sqlite import sqlite_connstr


dbconn = DBConn(sqlite_connstr())

dates_tbl = Table(
    name="dates",
    store=TableStoreDB(
        dbconn=dbconn, 
        name="dates", 
        data_sql_schema=[
            sa.Column("date_partition", sa.String, primary_key=True),
            sa.Column("hour", sa.Integer, primary_key=True),
            sa.Column('value', sa.Integer)
        ], 
        create_table=True
    )
)

dates_part_tbl = Table(
    name="dates_part",
    store=TableStoreDB(
        dbconn=dbconn, 
        name="dates_part", 
        data_sql_schema=[
            sa.Column("date_partition", sa.String, primary_key=True),
            sa.Column("hour", sa.Integer, primary_key=True),
            sa.Column("part", sa.String, primary_key=True),
            sa.Column('name', sa.String),
            sa.Column('value', sa.Integer)
        ], 
        create_table=True
    )
)

sums_tbl = Table(
    name="sums",
    store=TableStoreDB(
        dbconn=dbconn, 
        name="sums", 
        data_sql_schema=[
            sa.Column("date_partition", sa.String, primary_key=True),
            sa.Column("hour", sa.Integer, primary_key=True),
            sa.Column('sum', sa.Integer),
            sa.Column('part_sum', sa.Integer)
        ], 
        create_table=True
    )
)


def batch_generate_df() -> Generator[pd.DataFrame, None, None]:
    start_date = date(2025, 5, 1)
    end_date = date(2025, 5, 10)
    hours = [1, 4, 5, 6, 12, 18, 22]
    parts = ["1", "2"]

    dates = pd.date_range(start_date, end_date, freq="d")

    recotds = [
        {
            "date_partition": date.strftime("%Y-%m-%d"),
            "hour": hour,
            "value": hour + 1
        }
        for date in dates
        for hour in hours
    ]

    part_records = [
        {
            "date_partition": date.strftime("%Y-%m-%d"),
            "hour": hour,
            "part": part,
            "name": f"part_{str(part)}",
            "value": (hour + 1) * int(part)
        }
        for date in dates
        for hour in hours
        for part in parts
    ]

    yield pd.DataFrame(recotds), pd.DataFrame(part_records)


def chain_transform_dfs(
        dates: pd.DataFrame, 
        dates_part: pd.DataFrame, 
        previous_output: pd.DataFrame, 
        idx: pd.DataFrame
    ) -> pd.DataFrame:
    dates_part = dates_part.groupby(['date_partition', 'hour']).sum('value').reset_index()

    dates = dates.rename(columns={"value": "sum"})
    dates_part = dates_part.rename(columns={"value": "part_sum"})

    df_merged = (
        idx
        .merge(dates, on=['date_partition', 'hour'], how='left')
        .merge(dates_part, on=['date_partition', 'hour'], how='left')
    )
    previous_df = previous_output

    if not previous_df.empty:
        prev_df = previous_df.tail(1)[['date_partition', 'hour', "sum", "part_sum"]]
        df_merged = pd.concat([prev_df, df_merged])

    df_merged["part_sum"] = df_merged["part_sum"].cumsum()
    df_merged["sum"] = df_merged["sum"].cumsum()

    df_all = df_merged.merge(
        idx, 
        on=['date_partition', 'hour'], 
        how='left', 
        indicator=True
    )

    df = df_all[df_all['_merge'] == 'both'].drop(columns='_merge')

    return df 


def rank_function(**kwargs) -> int:
    date_partition = kwargs["date_partition"].replace("-", "")
    hour = kwargs["hour"]
    
    return int(date_partition) * 100 + int(hour)


executor_config = ExecutorConfig(parallelism=10)
pipeline = Pipeline(
    [
        BatchGenerate(
            batch_generate_df, 
            outputs=["dates", "dates_part"]
        ),
        ChainTransform(
             chain_transform_dfs,
             inputs=["dates", "dates_part"],
             previous=["sums"],
             outputs=["sums"],
             rank_func=rank_function,
             chunk_size=2,
             window_size=1,
             executor_config=executor_config
        ),
    ]
)

catalog = Catalog({
    "dates": dates_tbl,
    "dates_part": dates_part_tbl,
    "sums": sums_tbl,
})


ds = DataStore(dbconn)

app = DatapipeApp(ds, catalog, pipeline)
