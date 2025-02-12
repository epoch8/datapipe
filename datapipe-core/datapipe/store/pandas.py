from abc import ABC
from typing import Optional

import fsspec
import pandas as pd

from datapipe.sql_util import sql_schema_to_dtype
from datapipe.store.table_store import TableDataSingleFileStore
from datapipe.types import DataDF


class TableStoreExcel(TableDataSingleFileStore, ABC):
    def load_file(self) -> Optional[pd.DataFrame]:
        of = fsspec.open(self.filename)

        if of.fs.exists(of.path):
            dtypes = sql_schema_to_dtype(self.primary_schema)
            df = pd.read_excel(of.open(), engine="openpyxl", dtype=dtypes)

            return df
        else:
            return None

    def save_file(self, df: DataDF) -> None:
        with fsspec.open(self.filename, "wb+") as f:
            df.to_excel(f, engine="openpyxl", index=False)


class TableStoreJsonLine(TableDataSingleFileStore):
    def load_file(self) -> Optional[pd.DataFrame]:
        of = fsspec.open(self.filename)

        if of.fs.exists(of.path):
            dtypes = sql_schema_to_dtype(self.primary_schema)
            df = pd.read_json(of.open(), orient="records", lines=True, dtype=dtypes)

            return df
        else:
            return None

    def save_file(self, df: DataDF) -> None:
        with fsspec.open(self.filename, "w+") as f:
            df.to_json(f, orient="records", lines=True, force_ascii=False)
