from pytest_cases import parametrize_with_cases

from sqlalchemy import Column, String, Numeric
import pandas as pd

from datapipe.store.pandas import TableStoreJsonLine
from datapipe.store.table_store import TableStore
from datapipe.store.database import TableStoreDB

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


def case_table_store_db(dbconn):
    return TableStoreDB(dbconn, 'test', TEST_SCHEMA)


def case_table_store_json_line(tmp_dir):
    return TableStoreJsonLine(f'{tmp_dir}/test.jsonline')


@parametrize_with_cases('ts', cases='.')
def test_simple(ts: TableStore):
    ts.insert_rows(TEST_DF)

    res_df = ts.read_rows()

    assert_otm_df_equal(res_df, TEST_DF, TEST_INDEX_COLS)


@parametrize_with_cases('ts', cases='.')
def test_changed_pd_index(ts: TableStore):
    INSERT_DF = TEST_DF.copy()
    INSERT_DF.index += 1

    ts.insert_rows(INSERT_DF)

    res_df = ts.read_rows()

    assert_otm_df_equal(res_df, TEST_DF, TEST_INDEX_COLS)


@parametrize_with_cases('ts', cases='.')
def test_update(ts: TableStore):
    ts.insert_rows(TEST_DF)

    UPDATE_DF = TEST_DF.copy()
    UPDATE_DF.loc[0:5, 'a'] += 1

    ts.update_rows(UPDATE_DF.loc[0:5])

    res_df = ts.read_rows()

    assert_otm_df_equal(res_df, UPDATE_DF, TEST_INDEX_COLS)
