from abc import ABC
from typing import Optional, Union
from sqlalchemy import Column, String

import pandas as pd
from pathlib import Path

from datapipe.types import IndexDF, DataDF, DataSchema


class TableStore(ABC):
    def get_primary_schema(self) -> DataSchema:
        raise NotImplementedError

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

        По умолчанию используется read_rows
        '''
        return self.read_rows(idx)


class TableDataSingleFileStore(TableStore):

    def __init__(self, filename: Union[Path, str] = None, primary_schema: DataSchema = None):
        if not primary_schema:
            primary_schema = [Column("id", String(), primary_key=True)]

        self.primary_schema = primary_schema
        self.primary_keys = [column.name for column in primary_schema]
        self.filename = filename

    def get_primary_schema(self) -> DataSchema:
        return self.primary_schema

    def load_file(self) -> Optional[pd.DataFrame]:
        raise NotImplementedError

    def save_file(self, df: pd.DataFrame) -> None:
        raise NotImplementedError

    def read_rows(self, idx: Optional[IndexDF] = None) -> DataDF:
        file_df = self.load_file()

        if file_df is not None:
            if idx is not None:
                merged_df = file_df.merge(
                    idx,
                    how='left',
                    on=self.primary_keys,
                    indicator=True
                )

                exist_df = merged_df[merged_df['_merge'] == 'both'][file_df.columns]

                return exist_df.drop_duplicates()
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

    def delete_rows(self, idx: IndexDF) -> None:
        file_df = self.load_file()

        if file_df is not None:
            merged_df = file_df.merge(
                idx,
                how='left',
                on=self.primary_keys,
                indicator=True
            )

            new_df = merged_df[merged_df['_merge'] == 'left_only'][file_df.columns]

            self.save_file(new_df)

    def update_rows(self, df: DataDF) -> None:
        file_df = self.load_file()

        if set(self.primary_keys) - set(df.columns):
            raise ValueError("DataDf does not contains all primary keys")

        if file_df is None or file_df.empty:
            file_df = df
        else:
            data_cols = set(file_df.columns) - set(self.primary_keys)
            merged_df = file_df.merge(df, how="outer", on=self.primary_keys, suffixes=('', '_merge'))

            for column in data_cols:
                try:
                    merged_df[column] = merged_df[f'{column}_merge']
                except KeyError:
                    raise ValueError("DataDF does not contains all data columns for store")

            file_df = merged_df[list(file_df.columns)]

        check_df = file_df.drop_duplicates(subset=self.primary_keys)

        if len(file_df) > len(check_df):
            raise ValueError("DataDf contains duplicate rows by primary keys")

        self.save_file(file_df)
