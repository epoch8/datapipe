import pandas as pd
import sqlalchemy as sa

from datetime import date
from typing import Generator

from datapipe.compute import Catalog, DatapipeApp, Pipeline, Table
from datapipe.datatable import DataStore
from datapipe.step.batch_transform import BatchTransform
from datapipe.step.batch_generate import BatchGenerate
from datapipe.store.database import DBConn
from datapipe.store.filedir import PandasParquetFile, TableStoreFiledir

input_dfs_tbl = Table(
    name="input_dfs",
    store=TableStoreFiledir(
        "output/input_tbl/{date_partition}.parquet",
        PandasParquetFile(
            pandas_column="data"
        ),
        primary_schema=[
            sa.Column("date_partition", sa.String),
        ], 
    ),
)

output_dfs_tbl = Table(
    name="output_dfs",
    store=TableStoreFiledir(
        "output/output_tbl/{date_partition}.parquet",
        PandasParquetFile(
            pandas_column="data"
        ),
        primary_schema=[
            sa.Column("date_partition", sa.String),
        ], 
    ),
)


def batch_generate_df() -> Generator[pd.DataFrame, None, None]:
    start_date = date(2025, 5, 1)
    end_date = date(2025, 5, 10)

    dates = pd.date_range(start_date,end_date,freq='d')
    dfs = []

    for i in range(0, len(dates)):
        dfs.append(
            pd.DataFrame({"id": range(i+2), "size": [ 8 + j for j in range(i + 2)]})
        )

    yield pd.DataFrame({
        "date_partition": [date.strftime("%Y-%m-%d") for date in dates],
        "data": dfs
    })

def batch_preprocess_df(df: pd.DataFrame) -> pd.DataFrame:
    df["data"] = df["data"].apply(lambda df: df[df["size"] > 10])
    return df


pipeline = Pipeline(
    [
        BatchGenerate(batch_generate_df, outputs=[input_dfs_tbl]),
        BatchTransform(
            batch_preprocess_df,
            inputs=[input_dfs_tbl],
            outputs=[output_dfs_tbl],
            chunk_size=100,
        ),
    ]
)


ds = DataStore(DBConn("sqlite+pysqlite3:///db.sqlite"))

app = DatapipeApp(ds, Catalog({}), pipeline)