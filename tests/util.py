from typing import List
import tempfile
from pathlib import Path

import pytest
import pandas as pd


@pytest.fixture
def tmp_dir():
    with tempfile.TemporaryDirectory() as d:
        d = Path(d)
        yield d


def assert_idx_equal(a, b):
    a = sorted(list(a))
    b = sorted(list(b))

    assert(a == b)


def assert_df_equal(a: pd.DataFrame, b: pd.DataFrame) -> bool:
    assert_idx_equal(a.index, b.index)

    eq_rows = (a == b).all(axis='columns')

    if eq_rows.all():
        return True

    else:
        print('Difference')
        print('A:')
        print(a.loc[-eq_rows])
        print('B:')
        print(b.loc[-eq_rows])

        raise AssertionError


def assert_otm_df_equal(a: pd.DataFrame, b: pd.DataFrame, index_columns: List[str]) -> bool:
    a = a.set_index(index_columns).sort_index()
    b = b.set_index(index_columns).sort_index()

    assert_idx_equal(a.index, b.index)

    eq_rows = (a == b).all(axis='columns')

    if eq_rows.all():
        return True

    else:
        print('Difference')
        print('A:')
        print(a.loc[-eq_rows])
        print('B:')
        print(b.loc[-eq_rows])

        raise AssertionError
