from __future__ import annotations  # NOQA

from dataclasses import dataclass, field
from typing import Dict, List, NewType, TypeVar, cast

import pandas as pd
from sqlalchemy import Column

DataSchema = List[Column]
MetaSchema = List[Column]

# Dataframe with columns (<index_cols ...>)
IndexDF = NewType('IndexDF', pd.DataFrame)

# Dataframe with columns (<index_cols ...>, hash, create_ts, update_ts, process_ts, delete_ts)
MetadataDF = NewType('MetadataDF', pd.DataFrame)

# Dataframe with columns (<index_cols ...>, <data_cols ...>)
# DataDF = NewType('DataDF', pd.DataFrame)
DataDF = pd.DataFrame

TAnyDF = TypeVar("TAnyDF", pd.DataFrame, IndexDF, MetadataDF)


@dataclass
class ChangeList:
    changes: Dict[str, IndexDF] = field(default_factory=lambda: cast(Dict[str, IndexDF], {}))

    def append(self, table_name: str, idx: IndexDF) -> None:
        if table_name in self.changes:
            self_cols = set(self.changes[table_name].columns)
            other_cols = set(idx.columns)

            if self_cols != other_cols:
                raise ValueError(f"Different IndexDF for table {table_name}")

            self.changes[table_name] = cast(IndexDF, pd.concat([self.changes[table_name], idx], axis='index'))
        else:
            self.changes[table_name] = idx

    def extend(self, other: ChangeList):
        for key in other.changes.keys():
            self.append(key, other.changes[key])

    def empty(self):
        return len(self.changes.keys()) == 0

    @classmethod
    def create(cls, name: str, idx: IndexDF) -> ChangeList:
        changelist = cls()
        changelist.append(name, idx)

        return changelist


def data_to_index(data_df: DataDF, primary_keys: List[str]) -> IndexDF:
    return cast(IndexDF, data_df[primary_keys])


def meta_to_index(meta_df: MetadataDF, primary_keys: List[str]) -> IndexDF:
    return cast(IndexDF, meta_df[primary_keys])


def index_difference(idx1_df: IndexDF, idx2_df: IndexDF) -> IndexDF:
    assert(list(idx1_df.columns) == list(idx2_df.columns))
    cols = idx1_df.columns.to_list()

    idx1_idx = idx1_df.set_index(cols).index
    idx2_idx = idx2_df.set_index(cols).index

    return cast(IndexDF, idx1_idx.difference(idx2_idx).to_frame(index=False))


def index_intersection(idx1_df: IndexDF, idx2_df: IndexDF) -> IndexDF:
    assert(sorted(list(idx1_df.columns)) == sorted(list(idx2_df.columns)))
    cols = idx1_df.columns.to_list()

    idx1_idx = idx1_df.set_index(cols).index
    idx2_idx = idx2_df.set_index(cols).index

    return cast(IndexDF, idx1_idx.intersection(idx2_idx).to_frame(index=False))


def index_to_data(data_df: DataDF, idx_df: IndexDF) -> DataDF:
    idx_columns = list(idx_df.columns)
    data_df = data_df.set_index(idx_columns)
    indexes = idx_df.set_index(idx_columns)
    return cast(DataDF, data_df.loc[indexes.index].reset_index())
