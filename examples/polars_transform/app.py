import logging
from io import BytesIO
from typing import IO, Any, Dict

import polars as pl

from datapipe.compute import Catalog, DatapipeApp, Pipeline, Table
from datapipe.core_steps import BatchTransform, UpdateExternalTable
from datapipe.datatable import DataStore
from datapipe.store.database import DBConn
from datapipe.store.filedir import ItemStoreFileAdapter, TableStoreFiledir

logger = logging.getLogger("app")


class PolarsParquet(ItemStoreFileAdapter):
    mode = "b"

    def load(self, f: IO) -> Dict[str, Any]:
        assert isinstance(f, BytesIO)
        return {"data": pl.read_parquet(f)}

    def dump(self, obj: Dict[str, Any], f: IO) -> None:
        data = obj["data"]

        assert isinstance(data, pl.DataFrame)
        # assert isinstance(f, BytesIO)
        data.write_parquet(f)  # type: ignore


def polars_121(f):
    def wrapper(df):
        df["data"] = df["filepath"].apply(lambda fn: f(pl.read_parquet(fn)))
        return df

    return wrapper


ds = DataStore(DBConn("sqlite+pysqlite3:///db.sqlite"))


catalog = Catalog(
    {
        "input": Table(
            store=TableStoreFiledir(
                "input/{raw_input_chunk}.parquet",
                adapter=PolarsParquet(),
                add_filepath_column=True,
                read_data=False,
            )
        ),
        "output": Table(
            store=TableStoreFiledir(
                "output/{raw_input_chunk}.parquet",
                adapter=PolarsParquet(),
                add_filepath_column=True,
                read_data=False,
            )
        ),
    }
)


@polars_121
def parse_raw_input(df: pl.DataFrame) -> pl.DataFrame:
    return df


pipeline = Pipeline(
    [
        UpdateExternalTable(
            output="input",
            labels=[("stage", "update_external_table")],
        ),
        BatchTransform(
            func=lambda df: df,
            inputs=["input"],
            outputs=["output"],
            chunk_size=1,
        ),
    ]
)


app = DatapipeApp(ds=ds, catalog=catalog, pipeline=pipeline)
