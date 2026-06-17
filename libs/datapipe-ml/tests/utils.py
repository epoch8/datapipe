from __future__ import annotations

from typing import Iterable, Sequence

import pandas as pd


def assert_df_equal(
    actual: pd.DataFrame,
    expected: pd.DataFrame,
    index_cols: Sequence[str],
    *,
    check_dtype: bool = False,
) -> bool:
    actual_sorted = actual.sort_values(list(index_cols)).reset_index(drop=True)
    expected_sorted = expected.sort_values(list(index_cols)).reset_index(drop=True)
    pd.testing.assert_frame_equal(
        actual_sorted,
        expected_sorted,
        check_dtype=check_dtype,
    )
    return True


def assert_datatable_equal(dt, expected: pd.DataFrame, *, check_dtype: bool = False) -> bool:
    return assert_df_equal(dt.get_data(), expected, dt.primary_keys, check_dtype=check_dtype)


def assert_columns_present(df: pd.DataFrame, columns: Iterable[str]) -> None:
    missing = [column for column in columns if column not in df.columns]
    assert not missing, f"Missing columns: {missing}"


def assert_no_nulls(df: pd.DataFrame, columns: Iterable[str]) -> None:
    null_columns = [column for column in columns if df[column].isnull().any()]
    assert not null_columns, f"Columns contain null values: {null_columns}"
