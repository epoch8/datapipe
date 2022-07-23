import json

from datapipe.store.redis import RedisStore
from sqlalchemy import Column
from sqlalchemy import String, Integer
import pandas as pd

from .util import assert_df_equal


STORE_NAME = 'test_store'

TEST_SCHEMA_SINGLES = [
    Column('key', String(), primary_key=True),
    Column('value', Integer())
]

TEST_SCHEMA_MULTIPLES = [
    Column('key_1', String(), primary_key=True),
    Column('key_2', Integer(), primary_key=True),
    Column('value_1', String()),
    Column('value_2', Integer()),
]

TEST_DF_SINGLE_ROW = pd.DataFrame({'key': ['abc'], 'value': [123]})

TEST_DF_MULTIPLE_ROWS = pd.DataFrame({
        'key_1': ['abc', 'cbd'],
        'key_2': [123, 456],
        'value_1': ['row_1', 'row_2'],
        'value_2': [2, 3]
    })


def clear_redis(redis_conn):
    redis_conn.delete(STORE_NAME)


def test_insert_empty_df(redis_conn):
    clear_redis(redis_conn)
    store = RedisStore(redis_conn, STORE_NAME, TEST_SCHEMA_SINGLES)
    test_df = pd.DataFrame({'key': [], 'value': []})
    store.insert_rows(test_df)
    redis_content = redis_conn.hgetall(STORE_NAME)
    assert redis_content == {}


def test_insert_single_key_value_row_df(redis_conn):
    clear_redis(redis_conn)
    store = RedisStore(redis_conn, STORE_NAME, TEST_SCHEMA_SINGLES)
    store.insert_rows(TEST_DF_SINGLE_ROW)
    redis_content = redis_conn.hgetall(STORE_NAME)
    ser_key = json.dumps([TEST_DF_SINGLE_ROW['key'][0]])
    ser_val = json.dumps([int(TEST_DF_SINGLE_ROW['value'][0])])
    assert redis_content == {ser_key: ser_val}


def test_insert_multiple_keys_values_rows_df(redis_conn):
    clear_redis(redis_conn)
    store = RedisStore(redis_conn, STORE_NAME, TEST_SCHEMA_MULTIPLES)
    store.insert_rows(TEST_DF_MULTIPLE_ROWS)
    row_1_keys = json.dumps(
        tuple(TEST_DF_MULTIPLE_ROWS[['key_1', 'key_2']].itertuples(index=False, name=None))[0]
    )
    row_2_keys = json.dumps(
        tuple(TEST_DF_MULTIPLE_ROWS[['key_1', 'key_2']].itertuples(index=False, name=None))[1]
    )
    row_1_expected_vals = json.dumps(
        tuple(TEST_DF_MULTIPLE_ROWS[['value_1', 'value_2']].itertuples(index=False, name=None))[0]
    )
    row_2_expected_vals = json.dumps(
        tuple(TEST_DF_MULTIPLE_ROWS[['value_1', 'value_2']].itertuples(index=False, name=None))[1]
    )
    assert redis_conn.hget(STORE_NAME, row_1_keys) == row_1_expected_vals
    assert redis_conn.hget(STORE_NAME, row_2_keys) == row_2_expected_vals


def test_delete_rows(redis_conn):
    clear_redis(redis_conn)
    store = RedisStore(redis_conn, STORE_NAME, TEST_SCHEMA_SINGLES)
    store.insert_rows(TEST_DF_SINGLE_ROW)
    store.delete_rows(TEST_DF_SINGLE_ROW[['key']])
    assert redis_conn.hgetall(STORE_NAME) == {}


def test_read_all_rows(redis_conn):
    clear_redis(redis_conn)
    store = RedisStore(redis_conn, STORE_NAME, TEST_SCHEMA_MULTIPLES)
    store.insert_rows(TEST_DF_MULTIPLE_ROWS)
    df_result = store.read_rows()
    assert assert_df_equal(df_result, TEST_DF_MULTIPLE_ROWS, index_cols=['key_1', 'key_2'])


def test_read_rows_by_keys(redis_conn):
    clear_redis(redis_conn)
    store = RedisStore(redis_conn, STORE_NAME, TEST_SCHEMA_MULTIPLES)
    store.insert_rows(TEST_DF_MULTIPLE_ROWS)
    df_result = store.read_rows(TEST_DF_MULTIPLE_ROWS[['key_1', 'key_2']].iloc[:1])
    assert assert_df_equal(df_result, TEST_DF_MULTIPLE_ROWS.iloc[:1], index_cols=['key_1', 'key_2'])


def test_update_rows(redis_conn):
    clear_redis(redis_conn)
    store = RedisStore(redis_conn, STORE_NAME, TEST_SCHEMA_MULTIPLES)
    store.insert_rows(TEST_DF_MULTIPLE_ROWS)
    update_df = pd.DataFrame({
        'key_1': ['abc', 'cbd'],
        'key_2': [123, 456],
        'value_1': ['new_row_1', 'new_row_2'],
        'value_2': [4, 6]
    })
    store.update_rows(update_df)
    result_df = store.read_rows()
    assert assert_df_equal(result_df, update_df, index_cols=['key_1', 'key_2'])
