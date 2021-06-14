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
    return TableStoreDB(
        dbconn=dbconn,
        name='test',
        data_sql_schema=TEST_SCHEMA,
        index_columns=TEST_INDEX_COLS,
    )


def case_table_store_json_line(tmp_dir):
    return TableStoreJsonLine(
        filename=f'{tmp_dir}/test.jsonline',
        index_columns=TEST_INDEX_COLS,
    )


@parametrize_with_cases('ts', cases='.', prefix='case_table_store_')
def test_insert_read_no_filter(ts: TableStore):
    ts.insert_rows(TEST_DF)

    res_df = ts.read_rows()

    assert_otm_df_equal(res_df, TEST_DF, TEST_INDEX_COLS)


@parametrize_with_cases('ts', cases='.', prefix='case_table_store_')
def test_changed_df_index(ts: TableStore):
    INSERT_DF = TEST_DF.copy()
    INSERT_DF.index += 1

    ts.insert_rows(INSERT_DF)

    res_df = ts.read_rows()

    assert_otm_df_equal(res_df, TEST_DF, TEST_INDEX_COLS)


@parametrize_with_cases('ts', cases='.')
def test_delete_no_filter(ts: TableStore):
    ts.insert_rows(TEST_DF)

    DEL_DF = TEST_DF.loc[5:].set_index(pd.Index(range(5)))

    ts.delete_rows(TEST_DF[:5])

    res_df = ts.read_rows()
    assert_otm_df_equal(res_df, DEL_DF, TEST_INDEX_COLS)


@parametrize_with_cases('ts', cases='.')
def test_update_no_filter(ts: TableStore):
    ts.insert_rows(TEST_DF)

    UPDATE_DF = TEST_DF.copy()
    UPDATE_DF.loc[0:5, 'a'] += 1

    ts.update_rows(UPDATE_DF.loc[0:5])

    res_df = ts.read_rows()

    assert_otm_df_equal(res_df, UPDATE_DF, TEST_INDEX_COLS)
