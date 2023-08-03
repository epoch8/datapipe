from __future__ import annotations  # NOQA

import itertools
from dataclasses import dataclass, field
from typing import Callable, Dict, List, NewType, Set, Tuple, TypeVar, Union, cast

import pandas as pd
from sqlalchemy import Column

DataSchema = List[Column]
MetaSchema = List[Column]

# Dataframe with columns (<index_cols ...>)
IndexDF = NewType("IndexDF", pd.DataFrame)

# Dataframe with columns (<index_cols ...>, hash, create_ts, update_ts, process_ts, delete_ts)
MetadataDF = NewType("MetadataDF", pd.DataFrame)

# Dataframe with columns (<index_cols ...>, <data_cols ...>)
# DataDF = NewType('DataDF', pd.DataFrame)
DataDF = pd.DataFrame

TAnyDF = TypeVar("TAnyDF", pd.DataFrame, IndexDF, MetadataDF)

Labels = List[Tuple[str, str]]

TransformResult = Union[DataDF, List[DataDF], Tuple[DataDF, ...]]


@dataclass
class ChangeList:
    changes: Dict[str, IndexDF] = field(
        default_factory=lambda: cast(Dict[str, IndexDF], {})
    )

    def append(self, table_name: str, idx: IndexDF) -> None:
        if table_name in self.changes:
            self_cols = set(self.changes[table_name].columns)
            other_cols = set(idx.columns)

            if self_cols != other_cols:
                raise ValueError(f"Different IndexDF for table {table_name}")

            self.changes[table_name] = cast(
                IndexDF, pd.concat([self.changes[table_name], idx], axis="index")
            )
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
    assert sorted(idx1_df.columns) == sorted(idx2_df.columns)
    cols = idx1_df.columns.to_list()

    idx1_idx = idx1_df.set_index(cols).index
    idx2_idx = idx2_df.set_index(cols).index

    return cast(IndexDF, idx1_idx.difference(idx2_idx).to_frame(index=False))


def index_intersection(idx1_df: IndexDF, idx2_df: IndexDF) -> IndexDF:
    assert sorted(idx1_df.columns) == sorted(idx2_df.columns)
    cols = idx1_df.columns.to_list()

    idx1_idx = idx1_df.set_index(cols).index
    idx2_idx = idx2_df.set_index(cols).index

    return cast(IndexDF, idx1_idx.intersection(idx2_idx).to_frame(index=False))


def index_to_data(data_df: DataDF, idx_df: IndexDF) -> DataDF:
    idx_columns = list(idx_df.columns)
    data_df = data_df.set_index(idx_columns)
    indexes = idx_df.set_index(idx_columns)
    return cast(DataDF, data_df.loc[indexes.index].reset_index())


def get_pairwise_primary_intersections_in_tables(
    schemas: List[DataSchema], table_names: List[str]
) -> Dict[Tuple[str, str], Set[str]]:
    primary_keys = [
        set([x.name for x in schema if x.primary_key]) for schema in schemas
    ]
    idxs = range(len(schemas))
    pairs = itertools.combinations(idxs, 2)
    nt = lambda a, b: primary_keys[a].intersection(primary_keys[b])  # noqa
    pairwise_primary_intersections_in_tables = {
        (table_names[t[0]], table_names[t[1]]): nt(*t) for t in pairs
    }
    return pairwise_primary_intersections_in_tables


@dataclass
class PairIntersection:
    table_name: str
    idxs_intersection: List[str]


@dataclass
class TableWithDiffentPairsIntersection:
    table_name: str
    pairs_intersection: List[PairIntersection]


@dataclass
class EquivalenceTables:
    table_names: List[str]
    idxs: List[str]


def get_all_equivalence_tables(
    schemas: List[DataSchema], table_names: List[str]
) -> List[EquivalenceTables]:
    """
    Вычисляет классы эквивалетностей таблицы, максимально имеющих общие индексы
    """
    pairwise_primary_intersections_in_tables = (
        get_pairwise_primary_intersections_in_tables(schemas, table_names)
    )
    all_equalience_tables: List[EquivalenceTables] = []
    for table_name in table_names:
        # Проверяем, что у этой таблички должно быть непустым пересечение всех непустых пар
        non_empty_sets_intersection = None
        pairs = []
        for table_name1, table_name2 in pairwise_primary_intersections_in_tables:
            if table_name1 == table_name:
                pair_intersection = pairwise_primary_intersections_in_tables[
                    table_name, table_name2
                ]
                pairs.append((table_name, table_name2))
            elif table_name2 == table_name:
                pair_intersection = pairwise_primary_intersections_in_tables[
                    table_name1, table_name
                ]
                pairs.append((table_name1, table_name))
            else:
                continue
            if len(pair_intersection) > 0:
                if non_empty_sets_intersection is None:
                    non_empty_sets_intersection = pair_intersection
                else:
                    non_empty_sets_intersection = (
                        non_empty_sets_intersection.intersection(pair_intersection)
                    )
        if non_empty_sets_intersection is None:
            idxs = [
                x.name for x in schemas[table_names.index(table_name)] if x.primary_key
            ]
            all_equalience_tables.append(
                EquivalenceTables(table_names=[table_name], idxs=sorted(idxs))
            )
        if (
            non_empty_sets_intersection is not None
            and len(non_empty_sets_intersection) > 0
        ):
            have_idxs = False
            for tables_with_idxs in all_equalience_tables:
                for idx in non_empty_sets_intersection:
                    if idx in tables_with_idxs.idxs:
                        tables_with_idxs.idxs = sorted(
                            set(tables_with_idxs.idxs).intersection(
                                non_empty_sets_intersection
                            )
                        )
                        tables_with_idxs.table_names.append(table_name)
                        have_idxs = True
                if have_idxs:
                    break
            if not have_idxs:
                all_equalience_tables.append(
                    EquivalenceTables(
                        table_names=[table_name],
                        idxs=sorted(non_empty_sets_intersection),
                    )
                )
        if (
            non_empty_sets_intersection is not None
            and len(non_empty_sets_intersection) == 0
        ):
            raise ValueError(f"Table {table_name} has bad intersection with tables.")

    return all_equalience_tables


def safe_func_name(func: Callable) -> str:
    raw_name = func.__name__
    if raw_name == "<lambda>":
        return "lambda"
    return raw_name
