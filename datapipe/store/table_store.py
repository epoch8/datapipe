from abc import ABC
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, List, Optional, Union, cast

import cityhash
import pandas as pd
from sqlalchemy import Column, String

from datapipe.run_config import RunConfig
from datapipe.types import DataDF, DataSchema, HashDF, IndexDF, MetaSchema, data_to_index


@dataclass
class TableStoreCaps:
    supports_delete: bool
    supports_get_schema: bool
    supports_read_all_rows: bool
    supports_read_nonexistent_rows: bool
    supports_read_meta_pseudo_df: bool


class TableStore(ABC):
    caps: TableStoreCaps

    def get_primary_schema(self) -> DataSchema:
        raise NotImplementedError

    def get_meta_schema(self) -> MetaSchema:
        raise NotImplementedError

    def get_schema(self) -> DataSchema:
        raise NotImplementedError

    @property
    def primary_keys(self) -> List[str]:
        return [i.name for i in self.get_primary_schema()]

    @property
    def meta_keys(self) -> List[str]:
        return [i.name for i in self.get_meta_schema()]

    @property
    def hash_keys(self) -> List[str]:
        return self.primary_keys + self.meta_keys

    def hash_rows(self, df: DataDF) -> HashDF:
        hash_df = df[self.hash_keys]
        hash_df["hash"] = df.apply(lambda x: str(list(x)), axis=1).apply(
            lambda x: int.from_bytes(cityhash.CityHash32(x).to_bytes(4, "little"), "little", signed=True)
        )

        return cast(HashDF, hash_df)

    def delete_rows(self, idx: IndexDF) -> None:
        raise NotImplementedError

    def insert_rows(self, df: DataDF) -> None:
        raise NotImplementedError

    def update_rows(self, df: DataDF) -> None:
        if df.empty:
            return
        self.delete_rows(data_to_index(df, self.primary_keys))
        self.insert_rows(df)

    def read_rows(self, idx: Optional[IndexDF] = None) -> DataDF:
        raise NotImplementedError

    def read_rows_meta_pseudo_df(
        self, chunksize: int = 1000, run_config: Optional[RunConfig] = None
    ) -> Iterator[DataDF]:
        # FIXME сделать честную чанкированную реализацию во всех сторах
        yield self.read_rows()


class TableDataSingleFileStore(TableStore):
    caps = TableStoreCaps(
        supports_delete=True,
        supports_get_schema=False,
        supports_read_all_rows=True,
        supports_read_nonexistent_rows=True,
        supports_read_meta_pseudo_df=True,
    )

    def __init__(
        self,
        filename: Union[Path, str, None] = None,
        primary_schema: Optional[DataSchema] = None,
    ):
        if primary_schema is None:
            primary_schema = [Column("id", String(), primary_key=True)]

        self.primary_schema = primary_schema
        self.filename = filename

    def get_primary_schema(self) -> DataSchema:
        return self.primary_schema

    def get_meta_schema(self) -> MetaSchema:
        return []

    def load_file(self) -> Optional[DataDF]:
        raise NotImplementedError

    def save_file(self, df: DataDF) -> None:
        raise NotImplementedError

    def read_rows(self, idx: Optional[IndexDF] = None) -> DataDF:
        file_df = self.load_file()

        if file_df is not None:
            if idx is not None:
                if len(idx):
                    file_df = file_df.set_index(self.primary_keys)
                    idx = idx.set_index(self.primary_keys)

                    idx_to_read = file_df.index.intersection(idx.index)

                    return file_df.loc[idx_to_read].reset_index()
                else:
                    return pd.DataFrame(columns=self.primary_keys)
            else:
                return file_df
        else:
            return pd.DataFrame(columns=self.primary_keys)

    def insert_rows(self, df: DataDF) -> None:
        if df.empty:
            return

        self.delete_rows(data_to_index(df, self.primary_keys))

        file_df = self.load_file()

        if set(self.primary_keys) - set(df.columns.tolist()):
            raise ValueError("DataDf does not contains all primary keys")

        if file_df is None:
            new_df = df
        else:
            new_df = pd.concat([file_df, df], axis="index")

        check_df = new_df.drop_duplicates(subset=self.primary_keys)

        if len(new_df) > len(check_df):
            raise ValueError("DataDf contains duplicate rows by primary keys")

        self.save_file(new_df)

    def delete_rows(self, idx: IndexDF) -> None:
        file_df = self.load_file()

        if file_df is not None:
            file_df = file_df.set_index(self.primary_keys)
            idx = idx.set_index(self.primary_keys)

            new_df = file_df.loc[file_df.index.difference(idx.index)]

            self.save_file(new_df.reset_index())

    def update_rows(self, df: DataDF) -> None:
        self.insert_rows(df)
