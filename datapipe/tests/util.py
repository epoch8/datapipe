from typing import List, cast

import pandas as pd

from datapipe.datatable import DataTable
from datapipe.store.table_store import TableStore
from datapipe.types import DataDF, IndexDF, data_to_index


def assert_idx_equal(a, b):
    a = sorted(list(a))
    b = sorted(list(b))

    assert a == b


def assert_df_equal(a: pd.DataFrame, b: pd.DataFrame, index_cols=["id"]) -> bool:
    a = a.set_index(index_cols)
    b = b.set_index(index_cols)

    assert_idx_equal(a.index, b.index)

    eq_rows = (a.sort_index() == b.sort_index()).all(axis="columns")

    if eq_rows.all():
        return True

    else:
        print("Difference")
        print("A:")
        print(a.loc[-eq_rows])
        print("B:")
        print(b.loc[-eq_rows])

        raise AssertionError


def assert_datatable_equal(a: DataTable, b: DataDF) -> bool:
    return assert_df_equal(a.get_data(), b, index_cols=a.primary_keys)


def assert_ts_contains(ts: TableStore, df: DataDF):
    assert_df_equal(
        ts.read_rows(data_to_index(df, ts.primary_keys)),
        df,
        index_cols=ts.primary_keys,
    )


def assert_idx_no_duplicates(idx: IndexDF, index_cols: List[str]) -> bool:
    duplicates = cast(IndexDF, idx[idx[index_cols].duplicated()])
    if len(duplicates) == 0:
        return True
    else:
        idx = cast(IndexDF, idx.loc[idx.index].sort_values(index_cols))
        print("Duplicated found:")
        print(idx)

        raise AssertionError
