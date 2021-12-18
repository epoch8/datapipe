# Ex test_compute

# from typing import cast
# import pytest

import time
import pandas as pd
from sqlalchemy import Column
from sqlalchemy.sql.sqltypes import Integer

from datapipe.store.database import TableStoreDB
from datapipe.datatable import DataStore
from datapipe.core_steps import do_batch_generate, do_full_batch_transform, BatchTransformStep
from datapipe.types import ChangeList, IndexDF
from datapipe.run_config import RunConfig

from .util import assert_datatable_equal, assert_df_equal


TEST_SCHEMA1 = [
    Column('item_id', Integer, primary_key=True),
    Column('pipeline_id', Integer, primary_key=True),
    Column('a', Integer),
]

TEST_SCHEMA2 = [
    Column('item_id', Integer, primary_key=True),
    Column('a', Integer),
]

TEST_DF1_1 = pd.DataFrame(
    {
        'item_id': range(10),
        'pipeline_id': [i // 5 for i in range(10)],
        'a': range(10),
    },
)

TEST_DF1_2 = pd.DataFrame(
    {
        'item_id': list(range(5)) * 2,
        'pipeline_id': [i // 5 for i in range(10)],
        'a': range(10),
    },
)


def test_batch_transform(dbconn):
    ds = DataStore(dbconn)

    tbl1 = ds.create_table(
        'tbl1',
        table_store=TableStoreDB(dbconn, 'tbl1_data', TEST_SCHEMA1, True)
    )

    tbl2 = ds.create_table(
        'tbl2',
        table_store=TableStoreDB(dbconn, 'tbl2_data', TEST_SCHEMA1, True)
    )

    tbl1.store_chunk(TEST_DF1_1, now=0)

    do_full_batch_transform(
        func=lambda df: df,
        ds=ds,
        input_dts=[tbl1],
        output_dts=[tbl2],
    )

    meta_df = tbl2.get_metadata()

    update_ts = max(meta_df['update_ts'])
    process_ts = max(meta_df['process_ts'])

    time.sleep(0.1)

    do_full_batch_transform(
        func=lambda df: df,
        ds=ds,
        input_dts=[tbl1],
        output_dts=[tbl2],
    )

    meta_df = tbl2.get_metadata()

    assert(all(meta_df['update_ts'] == update_ts))
    assert(all(meta_df['process_ts'] == process_ts))


def test_batch_transform_with_filter(dbconn):
    ds = DataStore(dbconn)

    tbl1 = ds.create_table(
        'tbl1',
        table_store=TableStoreDB(dbconn, 'tbl1_data', TEST_SCHEMA1, True)
    )

    tbl2 = ds.create_table(
        'tbl2',
        table_store=TableStoreDB(dbconn, 'tbl2_data', TEST_SCHEMA1, True)
    )

    tbl1.store_chunk(TEST_DF1_1, now=0)

    do_full_batch_transform(
        func=lambda df: df,
        ds=ds,
        input_dts=[tbl1],
        output_dts=[tbl2],
        run_config=RunConfig(filters={'pipeline_id': 0})
    )

    assert_datatable_equal(tbl2, TEST_DF1_1.query('pipeline_id == 0'))


def test_batch_transform_with_filter_not_in_transform_index(dbconn):
    ds = DataStore(dbconn)

    tbl1 = ds.create_table(
        'tbl1',
        table_store=TableStoreDB(dbconn, 'tbl1_data', TEST_SCHEMA1, True)
    )

    tbl2 = ds.create_table(
        'tbl2',
        table_store=TableStoreDB(dbconn, 'tbl2_data', TEST_SCHEMA2, True)
    )

    tbl1.store_chunk(TEST_DF1_2, now=0)

    do_full_batch_transform(
        func=lambda df: df[['item_id', 'a']],
        ds=ds,
        input_dts=[tbl1],
        output_dts=[tbl2],
        run_config=RunConfig(filters={'pipeline_id': 0}),
    )

    assert_datatable_equal(tbl2, TEST_DF1_2.query('pipeline_id == 0')[['item_id', 'a']])


def test_batch_transform_with_dt_on_input_and_output(dbconn):
    ds = DataStore(dbconn)

    tbl1 = ds.create_table(
        'tbl1',
        table_store=TableStoreDB(dbconn, 'tbl1_data', TEST_SCHEMA1, True)
    )

    tbl2 = ds.create_table(
        'tbl2',
        table_store=TableStoreDB(dbconn, 'tbl2_data', TEST_SCHEMA1, True)
    )

    df2 = TEST_DF1_1.loc[range(3, 8)]
    df2['a'] = df2['a'].apply(lambda x: x + 10)

    tbl1.store_chunk(TEST_DF1_1, now=0)
    tbl2.store_chunk(df2, now=0)

    def update_df(df1: pd.DataFrame, df2: pd.DataFrame):
        df1 = df1.set_index("item_id")
        df2 = df2.set_index("item_id")

        df1.update(df2)

        return df1.reset_index()

    do_full_batch_transform(
        func=update_df,
        ds=ds,
        input_dts=[tbl1, tbl2],
        output_dts=[tbl2],
    )

    df_res = TEST_DF1_1.copy()

    df_res.update(df2)

    assert_datatable_equal(tbl2, df_res)


def test_gen_with_filter(dbconn):
    ds = DataStore(dbconn)

    tbl = ds.create_table(
        'tbl',
        table_store=TableStoreDB(dbconn, 'tbl_data', TEST_SCHEMA1, True)
    )

    tbl.store_chunk(TEST_DF1_1, now=0)

    def gen_func():
        yield TEST_DF1_1.query('pipeline_id == 0 and item_id == 0')

    do_batch_generate(
        func=gen_func,
        ds=ds,
        output_dts=[tbl],
        run_config=RunConfig(filters={'pipeline_id': 0})
    )

    assert_datatable_equal(tbl, TEST_DF1_1.query('(pipeline_id == 0 and item_id == 0) or pipeline_id == 1'))


def test_transform_with_changelist(dbconn):
    ds = DataStore(dbconn)

    tbl1 = ds.create_table(
        'tbl1',
        table_store=TableStoreDB(dbconn, 'tbl1_data', TEST_SCHEMA1, True)
    )

    tbl2 = ds.create_table(
        'tbl2',
        table_store=TableStoreDB(dbconn, 'tbl2_data', TEST_SCHEMA1, True)
    )

    tbl1.store_chunk(TEST_DF1_1, now=0)

    def func(df):
        return df

    step = BatchTransformStep(
        'test',
        func=func,
        input_dts=[tbl1],
        output_dts=[tbl2]
    )

    change_list = ChangeList()

    idx_keys = ['item_id', 'pipeline_id']
    changes_df = TEST_DF1_1.loc[[0, 1, 2]]
    changes_idx = IndexDF(changes_df[idx_keys])

    change_list.append('tbl1', changes_idx)

    next_change_list = step.run_changelist(ds, change_list)

    assert_datatable_equal(tbl2, changes_df)

    assert list(next_change_list.changes.keys()) == ['tbl2']

    assert_df_equal(next_change_list.changes['tbl2'], changes_idx, index_cols=idx_keys)
