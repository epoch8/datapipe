from abc import ABC
from typing import Optional, Union
from dataclasses import dataclass

import pandas as pd
from pathlib import Path

from datapipe.store.types import Index


class TableDataStore(ABC):
    def delete_rows(self, idx: Index) -> None:
        raise NotImplementedError

    def insert_rows(self, df: pd.DataFrame) -> None:
        raise NotImplementedError

    def update_rows(self, df: pd.DataFrame) -> None:
        self.delete_rows(df.index)
        self.insert_rows(df)

    def read_rows(self, idx: Optional[Index] = None) -> pd.DataFrame:
        raise NotImplementedError

    def read_rows_meta_pseudo_df(self, idx: Optional[Index] = None) -> pd.DataFrame:
        '''
        Подготовить датафрейм с "какбы данными" на основе которых посчитается хеш и обновятся метаданные

        По умолчанию используется read_rows
        '''
        return self.read_rows(idx)


@dataclass
class TableDataSingleFileStore(TableDataStore):
    filename: Union[Path, str]

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
            new_df = file_df.loc[file_df.index.difference(idx)]

            self.save_file(new_df)

    def update_rows(self, df: pd.DataFrame) -> None:
        file_df = self.load_file()

        if file_df is None:
            file_df = df
        else:
            file_df.loc[df.index] = df

        self.save_file(file_df)
