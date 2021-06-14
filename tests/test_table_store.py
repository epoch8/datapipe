from datapipe.store.pandas import TableStoreJsonLine
from datapipe.store.table_store import TableStore
import pytest

from datapipe.store.database import TableStoreDB
from sqlalchemy import Column, String, Numeric
import pandas as pd

from .util import assert_otm_df_equal

TEST_SCHEMA = [
    Column('id', String(100)),
    Column('a', Numeric),
]

TEST_DF = pd.DataFrame(
    {
        'id': [f'id_{i}' for i in range(10)],
        'a': range(10)
    },
)

TEST_INDEX_COLS = ['id']


def make_table_store_db(dbconn, tmp_dir):
    return TableStoreDB(dbconn, 'test', TEST_SCHEMA)


def make_table_store_json_line(dbconn, tmp_dir):
    return TableStoreJsonLine(f'{tmp_dir}/test.jsonline')


# TODO Посмотреть на https://smarie.github.io/python-pytest-cases/
# TODO добавить сюда все реализации TableStore
@pytest.fixture(params=[
    make_table_store_db,
    make_table_store_json_line,
])
def ts(dbconn, tmp_dir, request):
    return request.param(dbconn, tmp_dir)


def test_simple(ts: TableStore):
    ts.insert_rows(TEST_DF)

    res_df = ts.read_rows()

    assert_otm_df_equal(res_df, TEST_DF, TEST_INDEX_COLS)


def test_changed_pd_index(ts: TableStore):
    INSERT_DF = TEST_DF.copy()
    INSERT_DF.index += 1

    ts.insert_rows(INSERT_DF)

    res_df = ts.read_rows()

    assert_otm_df_equal(res_df, TEST_DF, TEST_INDEX_COLS)


def test_update(ts: TableStore):
    ts.insert_rows(TEST_DF)

    UPDATE_DF = TEST_DF.copy()
    UPDATE_DF.loc[0:5, 'a'] += 1

    ts.update_rows(UPDATE_DF.loc[0:5])

    res_df = ts.read_rows()

    assert_otm_df_equal(res_df, UPDATE_DF, TEST_INDEX_COLS)
