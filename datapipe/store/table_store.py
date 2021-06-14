from abc import ABC
from typing import Optional, Union
from dataclasses import dataclass

import pandas as pd
from pathlib import Path

from datapipe.store.types import Index, IndexMeta


class TableStore(ABC):
    def __init__(self, index_columns: IndexMeta) -> None:
        self.index_columns = index_columns

    def delete_rows(self, idx: Index) -> None:
        raise NotImplementedError

    def insert_rows(self, df: pd.DataFrame) -> None:
        raise NotImplementedError

    def update_rows(self, df: pd.DataFrame) -> None:
        self.delete_rows(df[[self.index_columns]])
        self.insert_rows(df)

    def read_rows(self, idx: Optional[Index] = None) -> pd.DataFrame:
        raise NotImplementedError

    def read_rows_meta_pseudo_df(self, idx: Optional[Index] = None) -> pd.DataFrame:
        '''
        Подготовить датафрейм с "какбы данными" на основе которых посчитается хеш и обновятся метаданные

        По умолчанию используется read_rows
        '''
        return self.read_rows(idx)


class TableDataSingleFileStore(TableStore):
    def __init__(self, filename: Union[Path, str], index_columns: IndexMeta) -> None:
        super().__init__(index_columns)
        self.filename = filename

    def load_file(self) -> Optional[pd.DataFrame]:
        raise NotImplementedError

    def save_file(self, df: pd.DataFrame) -> None:
        raise NotImplementedError

    def read_rows(self, idx: Optional[Index] = None) -> pd.DataFrame:
        file_df = self.load_file()

        if file_df is not None:
            if idx is not None:
                return file_df.loc[idx]
            else:
                return file_df

        else:
            return pd.DataFrame()

    def insert_rows(self, df: pd.DataFrame) -> None:
        file_df = self.load_file()

        if file_df is None:
            new_df = df
        else:
            new_df = file_df.append(df)

        self.save_file(new_df)

    def delete_rows(self, idx: Index) -> None:
        file_df = self.load_file()

        if file_df is not None:
            file_df = file_df.set_index(self.index_columns)
            idx = idx.set_index(self.index_columns)

            new_df = file_df.loc[file_df.index.difference(idx.index)]

            self.save_file(new_df.reset_index())

    def update_rows(self, df: pd.DataFrame) -> None:
        file_df = self.load_file()

        if file_df is None:
            file_df = df
        else:
            file_df.loc[df.index] = df

        self.save_file(file_df)
