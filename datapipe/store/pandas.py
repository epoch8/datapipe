import datetime
from abc import ABC

import fsspec
import pandas as pd

from datapipe.sql_util import sql_schema_to_dtype
from datapipe.store.table_store import TableDataSingleFileStore
from datapipe.types import DataDF

_DATETIME_PYTHON_TYPES = {datetime.datetime, datetime.date, datetime.time}


class TableStoreExcel(TableDataSingleFileStore, ABC):
    def load_file(self) -> pd.DataFrame | None:
        of = fsspec.open(self.filename)

        if of.fs.exists(of.path):
            dtypes = sql_schema_to_dtype(self.primary_schema)
            plain_dtypes = {k: v for k, v in dtypes.items() if v not in _DATETIME_PYTHON_TYPES}

            df = pd.read_excel(of.open(), engine="openpyxl", dtype=plain_dtypes)

            for col, py_type in dtypes.items():
                if col not in df.columns or py_type not in _DATETIME_PYTHON_TYPES:
                    continue
                if py_type == datetime.datetime:
                    df[col] = pd.to_datetime(df[col])
                elif py_type == datetime.date:
                    df[col] = pd.to_datetime(df[col]).dt.date
                elif py_type == datetime.time:
                    df[col] = pd.to_datetime(df[col]).dt.time

            return df
        else:
            return None

    def save_file(self, df: DataDF) -> None:
        with fsspec.open(self.filename, "wb+") as f:
            df.to_excel(f, engine="openpyxl", index=False)


class TableStoreJsonLine(TableDataSingleFileStore):
    def load_file(self) -> pd.DataFrame | None:
        of = fsspec.open(self.filename)

        if of.fs.exists(of.path):
            dtypes = sql_schema_to_dtype(self.primary_schema)
            datetime_cols = [k for k, v in dtypes.items() if v in _DATETIME_PYTHON_TYPES]
            plain_dtypes = {k: v for k, v in dtypes.items() if v not in _DATETIME_PYTHON_TYPES}

            df = pd.read_json(
                of.open(),
                orient="records",
                lines=True,
                dtype=plain_dtypes,
                convert_dates=datetime_cols if datetime_cols else False,
            )

            for col, py_type in dtypes.items():
                if col not in df.columns:
                    continue
                if py_type == datetime.date:
                    df[col] = pd.to_datetime(df[col]).dt.date
                elif py_type == datetime.time:
                    df[col] = pd.to_datetime(df[col]).dt.time

            return df
        else:
            return None

    def save_file(self, df: DataDF) -> None:
        with fsspec.open(self.filename, "w+") as f:
            df.to_json(f, orient="records", lines=True, force_ascii=False, date_format="iso")
