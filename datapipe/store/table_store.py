from abc import ABC
from typing import Any, Dict, List, Optional, Union, cast

from sqlalchemy import Column, String
import pandas as pd
from pathlib import Path

from datapipe.types import IndexDF, DataDF, DataSchema


def append_missing_keys_to_empty_df(data_df: DataDF, primary_keys: List[str], dtypes: Dict[str, Any]) -> DataDF:
    assert data_df.empty

    for key in [key for key in primary_keys if key not in data_df.columns]:
        data_df[key] = pd.Series([], dtype=dtypes[key])
    data_df = cast(DataDF, data_df.astype(dtypes))

    return data_df


class TableStore(ABC):
    def get_primary_schema(self) -> DataSchema:
        raise NotImplementedError

    @property
    def primary_keys(self) -> List[str]:
        return [i.name for i in self.get_primary_schema()]

    def delete_rows(self, idx: IndexDF) -> None:
        raise NotImplementedError

    def insert_rows(self, df: DataDF) -> None:
        raise NotImplementedError

    def update_rows(self, df: DataDF) -> None:
        self.delete_rows(df.index)
        self.insert_rows(df)

    def read_rows(self, idx: IndexDF = None) -> DataDF:
        raise NotImplementedError

    def read_rows_meta_pseudo_df(self, idx: Optional[IndexDF] = None) -> DataDF:
        '''
        Подготовить датафрейм с "какбы данными" на основе которых посчитается хеш и обновятся метаданные
        '''

        # TODO переделать на чанкированную обработку
        return self.read_rows(idx)


class TableDataSingleFileStore(TableStore):

    def __init__(self, filename: Union[Path, str] = None, primary_schema: DataSchema = None):
        if not primary_schema:
            primary_schema = [Column("id", String(), primary_key=True)]

        self.primary_schema = primary_schema
        self.filename = filename

    def get_primary_schema(self) -> DataSchema:
        return self.primary_schema

    def load_file(self) -> Optional[DataDF]:
        raise NotImplementedError

    def save_file(self, df: DataDF) -> None:
        raise NotImplementedError

    def _load_file(self) -> Optional[DataDF]:
        file_df = self.load_file()

        # Заполняем пустую табличку ключами, если они не оказалось записанными:
        from datapipe.store.database import sql_schema_to_dtype
        if file_df is not None and file_df.empty:
            file_df = append_missing_keys_to_empty_df(
                file_df, self.primary_keys, sql_schema_to_dtype(self.get_primary_schema())
            )
        return file_df

    def read_rows(self, index_df: Optional[IndexDF] = None) -> DataDF:
        file_df = self.load_file()

        if file_df is not None:
            if index_df is not None:
                file_df = file_df.set_index(self.primary_keys)
                idx = index_df.set_index(self.primary_keys)

                return file_df.loc[idx.index].reset_index()
            else:
                return file_df
        else:
            return pd.DataFrame()

    def insert_rows(self, df: DataDF) -> None:
        file_df = self.load_file()

        if set(self.primary_keys) - set(df.columns):
            raise ValueError("DataDf does not contains all primary keys")

        if file_df is None:
            new_df = df
        else:
            new_df = file_df.append(df)

        check_df = new_df.drop_duplicates(subset=self.primary_keys)

        if len(new_df) > len(check_df):
            raise ValueError("DataDf contains duplicate rows by primary keys")

        self.save_file(new_df)

    def delete_rows(self, index_df: IndexDF) -> None:
        file_df = self.load_file()

        if file_df is not None:
            file_df = file_df.set_index(self.primary_keys)
            idx = index_df.set_index(self.primary_keys)

            new_df = file_df.loc[file_df.index.difference(idx.index)]

            self.save_file(new_df.reset_index())

    def update_rows(self, df: DataDF) -> None:
        file_df = self.load_file()

        if set(self.primary_keys) - set(df.columns):
            raise ValueError("DataDf does not contains all primary keys")

        if file_df is None or file_df.empty:
            file_df = df
        else:
            file_df = file_df.set_index(self.primary_keys)
            df = df.set_index(self.primary_keys)

            file_df.loc[df.index] = df

            file_df = file_df.reset_index()

        self.save_file(file_df)
