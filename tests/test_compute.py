# from typing import cast
# import pytest

import time
import pandas as pd
from sqlalchemy import Column
from sqlalchemy.sql.sqltypes import Integer

from datapipe.store.database import TableStoreDB
from datapipe.datatable import DataStore
from datapipe.compute import BatchGenerateStep, BatchTransformStep
from datapipe.step import RunConfig

from .util import assert_datatable_equal


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

    bt_step = BatchTransformStep(
        name='tbl1_to_tbl2',
        func=lambda df: df,
        input_dts=[tbl1],
        output_dts=[tbl2],
        chunk_size=1000,
    )

    bt_step.run(ds)

    meta_df = tbl2.get_metadata()

    update_ts = max(meta_df['update_ts'])
    process_ts = max(meta_df['process_ts'])

    time.sleep(0.1)

    bt_step.run(ds)

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

    bt_step = BatchTransformStep(
        name='tbl1_to_tbl2',
        func=lambda df: df,
        input_dts=[tbl1],
        output_dts=[tbl2],
        chunk_size=1000,
    )

    bt_step.run(ds, run_config=RunConfig(filters={'pipeline_id': 0}))

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

    bt_step = BatchTransformStep(
        name='tbl1_to_tbl2',
        func=lambda df: df[['item_id', 'a']],
        input_dts=[tbl1],
        output_dts=[tbl2],
        chunk_size=1000,
    )

    bt_step.run(ds, run_config=RunConfig(filters={'pipeline_id': 0}))

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

    bt_step = BatchTransformStep(
        name='tbl1_to_tbl1',
        func=update_df,
        input_dts=[tbl1, tbl2],
        output_dts=[tbl2],
        chunk_size=1000,
    )

    bt_step.run(ds, run_config=RunConfig())

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

    gen_step = BatchGenerateStep(
        name='gen_tbl',
        func=gen_func,
        input_dts=[],
        output_dts=[tbl],
    )

    gen_step.run(ds, run_config=RunConfig(filters={'pipeline_id': 0}))

    assert_datatable_equal(tbl, TEST_DF1_1.query('(pipeline_id == 0 and item_id == 0) or pipeline_id == 1'))
