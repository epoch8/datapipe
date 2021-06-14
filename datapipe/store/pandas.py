from abc import ABC
from dataclasses import dataclass
from typing import Callable, Optional

import fsspec
import pandas as pd

from datapipe.store.table_store import TableDataSingleFileStore


@dataclass
class TableStoreExcel(TableDataSingleFileStore, ABC):
    read_adapter: Optional[Callable[[pd.DataFrame], pd.DataFrame]] = None

    def load_file(self) -> pd.DataFrame:
        with fsspec.open(self.filename) as f:
            df = pd.read_excel(f, engine='openpyxl')

        if self.read_adapter is not None:
            df = self.read_adapter(df)

        return df


@dataclass
class TableStoreJsonLine(TableDataSingleFileStore):
    def load_file(self) -> Optional[pd.DataFrame]:
        of = fsspec.open(self.filename)

        if of.fs.exists(of.path):
            df = pd.read_json(of.open(), orient='records', lines=True, dtype={'id': str})
            df = df.set_index('id')

            return df
        else:
            return None

    def save_file(self, df: pd.DataFrame) -> None:
        df.index.rename('id', inplace=True)
        with fsspec.open(self.filename, 'w+') as f:
            df.reset_index().to_json(f, orient='records', lines=True)
