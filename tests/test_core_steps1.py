# Ex-test_datatable

from typing import cast
import pytest

import pandas as pd
from sqlalchemy import Column
from sqlalchemy.sql.sqltypes import Integer

from datapipe.store.database import TableStoreDB
from datapipe.datatable import DataStore
from datapipe.core_steps import BatchGenerateStep, BatchTransformIncStep
from datapipe.types import IndexDF, data_to_index

from .util import assert_df_equal, assert_datatable_equal


TEST_SCHEMA = [
    Column('id', Integer, primary_key=True),
    Column('a', Integer),
]

TEST_DF = pd.DataFrame(
    {
        'id': range(10),
        'a': range(10),
    },
)


TEST_DF_INC1 = TEST_DF.assign(a=lambda df: df['a'] + 1)
TEST_DF_INC2 = TEST_DF.assign(a=lambda df: df['a'] + 2)
TEST_DF_INC3 = TEST_DF.assign(a=lambda df: df['a'] + 3)


def yield_df(data):
    def f(*args, **kwargs):
        yield pd.DataFrame.from_records(data, columns=['id', 'a']).set_index('id')

    return f


def test_gen_process(dbconn) -> None:
    ds = DataStore(dbconn)

    tbl1 = ds.create_table(
        'tbl1',
        table_store=TableStoreDB(dbconn, 'tbl1_data', TEST_SCHEMA, True)
    )

    def gen():
        yield TEST_DF

    BatchGenerateStep(
        name='tbl1_gen',
        func=gen,
        output_dts=[tbl1]
    ).run(ds)

    assert_datatable_equal(tbl1, TEST_DF)

    def func():
        return TEST_DF

    with pytest.raises(Exception):
        BatchGenerateStep(
            name='tbl1',
            func=func,
            output_dts=[tbl1]
        ).run(ds)


def test_inc_process_modify_values(dbconn) -> None:
    ds = DataStore(dbconn)

    tbl1 = ds.create_table(
        'tbl1',
        table_store=TableStoreDB(dbconn, 'tbl1_data', TEST_SCHEMA, True)
    )
    tbl2 = ds.create_table(
        'tbl2',
        table_store=TableStoreDB(dbconn, 'tbl2_data', TEST_SCHEMA, True)
    )

    def id_func(df):
        return df

    tbl1.store_chunk(TEST_DF)

    step = BatchTransformIncStep(
        name='inc_step',
        input_dts=[tbl1],
        output_dts=[tbl2],
        func=id_func
    )

    step.run(ds)

    assert_datatable_equal(tbl2, TEST_DF)

    ##########################
    tbl1.store_chunk(TEST_DF_INC1)

    step.run(ds)

    assert_datatable_equal(tbl2, TEST_DF_INC1)


def test_inc_process_delete_values_from_input(dbconn) -> None:
    ds = DataStore(dbconn)

    tbl1 = ds.create_table(
        'tbl1',
        table_store=TableStoreDB(dbconn, 'tbl1_data', TEST_SCHEMA, True)
    )
    tbl2 = ds.create_table(
        'tbl2',
        table_store=TableStoreDB(dbconn, 'tbl2_data', TEST_SCHEMA, True)
    )

    def id_func(df):
        return df

    tbl1.store_chunk(TEST_DF)

    BatchTransformIncStep(
        name='inc',
        input_dts=[tbl1],
        output_dts=[tbl2],
        func=id_func
    ).run(ds)

    assert_datatable_equal(tbl2, TEST_DF)

    ##########################
    tbl1.store_chunk(TEST_DF[:5], processed_idx=data_to_index(TEST_DF, tbl1.primary_keys))

    BatchTransformIncStep(
        name='inc',
        input_dts=[tbl1],
        output_dts=[tbl2],
        func=id_func,
        chunk_size=2
    ).run(ds)

    assert_datatable_equal(tbl2, TEST_DF[:5])


def test_inc_process_delete_values_from_proc(dbconn) -> None:
    ds = DataStore(dbconn)

    tbl1 = ds.create_table(
        'tbl1',
        table_store=TableStoreDB(dbconn, 'tbl1_data', TEST_SCHEMA, True)
    )
    tbl2 = ds.create_table(
        'tbl2',
        table_store=TableStoreDB(dbconn, 'tbl2_data', TEST_SCHEMA, True)
    )

    def id_func(df):
        return df[:5]

    tbl2.store_chunk(TEST_DF)

    tbl1.store_chunk(TEST_DF)

    BatchTransformIncStep(
        name='inc',
        input_dts=[tbl1],
        output_dts=[tbl2],
        func=id_func
    ).run(ds)

    assert_datatable_equal(tbl2, TEST_DF[:5])


def test_inc_process_proc_no_change(dbconn) -> None:
    ds = DataStore(dbconn)

    tbl1 = ds.create_table(
        'tbl1',
        table_store=TableStoreDB(dbconn, 'tbl1_data', TEST_SCHEMA, True)
    )
    tbl2 = ds.create_table(
        'tbl2',
        table_store=TableStoreDB(dbconn, 'tbl2_data', TEST_SCHEMA, True)
    )

    def id_func(df):
        return TEST_DF

    tbl2.store_chunk(TEST_DF)
    tbl1.store_chunk(TEST_DF)

    count, idx_gen = ds.get_process_ids([tbl1], [tbl2])
    idx_dfs = list(idx_gen)
    idx_len = len(pd.concat(idx_dfs)) if len(idx_dfs) > 0 else 0

    assert(idx_len == len(TEST_DF))

    step = BatchTransformIncStep(
        name='transform',
        input_dts=[tbl1],
        output_dts=[tbl2],
        func=id_func
    )

    step.run(ds)

    count, idx_gen = ds.get_process_ids([tbl1], [tbl2])
    idx_dfs = list(idx_gen)
    idx_len = len(pd.concat(idx_dfs)) if len(idx_dfs) > 0 else 0

    assert(idx_len == 0)

    tbl1.store_chunk(TEST_DF_INC1)

    count, idx_gen = ds.get_process_ids([tbl1], [tbl2])
    idx_dfs = list(idx_gen)
    idx_len = len(pd.concat(idx_dfs)) if len(idx_dfs) > 0 else 0

    assert(idx_len == len(TEST_DF))

    step.run(ds)

    count, idx_gen = ds.get_process_ids([tbl1], [tbl2])
    idx_dfs = list(idx_gen)
    idx_len = len(pd.concat(idx_dfs)) if len(idx_dfs) > 0 else 0

    assert(idx_len == 0)

# TODO тест inc_process 2->1
# TODO тест inc_process 2->1, удаление строки, 2->1


def test_gen_process_many(dbconn) -> None:
    ds = DataStore(dbconn)

    tbl_gen = ds.create_table(
        'tbl_gen',
        table_store=TableStoreDB(dbconn, 'tbl_gen_data', TEST_SCHEMA, True)
    )
    tbl1_gen = ds.create_table(
        'tbl1_gen',
        table_store=TableStoreDB(dbconn, 'tbl1_gen_data', TEST_SCHEMA, True)
    )
    tbl2_gen = ds.create_table(
        'tbl2_gen',
        table_store=TableStoreDB(dbconn, 'tbl2_gen_data', TEST_SCHEMA, True)
    )
    tbl3_gen = ds.create_table(
        'tbl3_gen',
        table_store=TableStoreDB(dbconn, 'tbl3_gen_data', TEST_SCHEMA, True)
    )

    def gen():
        yield (TEST_DF, TEST_DF_INC1, TEST_DF_INC2, TEST_DF_INC3)

    BatchGenerateStep(
        name='gen',
        output_dts=[tbl_gen, tbl1_gen, tbl2_gen, tbl3_gen],
        func=gen
    ).run(ds)

    assert_datatable_equal(tbl_gen, TEST_DF)
    assert_datatable_equal(tbl1_gen, TEST_DF_INC1)
    assert_datatable_equal(tbl2_gen, TEST_DF_INC2)
    assert_datatable_equal(tbl3_gen, TEST_DF_INC3)


def test_inc_process_many_modify_values(dbconn) -> None:
    ds = DataStore(dbconn)

    tbl = ds.create_table(
        'tbl',
        table_store=TableStoreDB(dbconn, 'tbl_data', TEST_SCHEMA, True)
    )
    tbl1 = ds.create_table(
        'tbl1',
        table_store=TableStoreDB(dbconn, 'tbl1_data', TEST_SCHEMA, True)
    )
    tbl2 = ds.create_table(
        'tbl2',
        table_store=TableStoreDB(dbconn, 'tbl2_data', TEST_SCHEMA, True)
    )
    tbl3 = ds.create_table(
        'tbl3',
        table_store=TableStoreDB(dbconn, 'tbl3_data', TEST_SCHEMA, True)
    )

    def inc_func(df):
        df1 = df.copy()
        df2 = df.copy()
        df3 = df.copy()
        df1['a'] += 1
        df2['a'] += 2
        df3['a'] += 3
        return df1, df2, df3

    tbl.store_chunk(TEST_DF)

    BatchTransformIncStep(
        name='transform',
        input_dts=[tbl],
        output_dts=[tbl1, tbl2, tbl3],
        func=inc_func
    ).run(ds)

    assert_datatable_equal(tbl1, TEST_DF_INC1)
    assert_datatable_equal(tbl2, TEST_DF_INC2)
    assert_datatable_equal(tbl3, TEST_DF_INC3)

    ##########################
    tbl.store_chunk(TEST_DF[:5], processed_idx=data_to_index(TEST_DF, tbl.primary_keys))

    def inc_func_inv(df):
        df1 = df.copy()
        df2 = df.copy()
        df3 = df.copy()
        df1['a'] += 1
        df2['a'] += 2
        df3['a'] += 3
        return df3, df2, df1

    BatchTransformIncStep(
        name='transform',
        input_dts=[tbl],
        output_dts=[tbl3, tbl2, tbl1],
        func=inc_func_inv
    ).run(ds)

    assert_datatable_equal(tbl1, TEST_DF_INC1[:5])
    assert_datatable_equal(tbl2, TEST_DF_INC2[:5])
    assert_datatable_equal(tbl3, TEST_DF_INC3[:5])

    ##########################

    tbl.store_chunk(TEST_DF[5:])

    BatchTransformIncStep(
        name='transform',
        input_dts=[tbl],
        output_dts=[tbl1, tbl2, tbl3],
        func=inc_func
    ).run(ds)

    assert_datatable_equal(tbl1, TEST_DF_INC1)
    assert_datatable_equal(tbl2, TEST_DF_INC2)
    assert_datatable_equal(tbl3, TEST_DF_INC3)


def test_inc_process_many_several_inputs(dbconn) -> None:
    ds = DataStore(dbconn)

    tbl = ds.create_table(
        'tbl',
        table_store=TableStoreDB(
            dbconn,
            'tbl_data',
            [
                Column('id', Integer, primary_key=True),
                Column('a_first', Integer),
                Column('a_second', Integer)
            ],
            True
        )
    )
    tbl1 = ds.create_table(
        'tbl1',
        table_store=TableStoreDB(dbconn, 'tbl1_data', TEST_SCHEMA, True)
    )
    tbl2 = ds.create_table(
        'tbl2',
        table_store=TableStoreDB(dbconn, 'tbl2_data', TEST_SCHEMA, True)
    )

    def inc_func(df1, df2):
        df = pd.merge(
            left=df1,
            right=df2,
            on=['id'],
            suffixes=('_first', '_second')
        )
        df['a_first'] += 1
        df['a_second'] += 2
        return df

    tbl1.store_chunk(TEST_DF)
    tbl2.store_chunk(TEST_DF)

    BatchTransformIncStep(
        name='transform',
        input_dts=[tbl1, tbl2],
        output_dts=[tbl],
        func=inc_func
    ).run(ds)

    assert_datatable_equal(
        tbl,
        pd.DataFrame(
            {
                'id': [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
                'a_first': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
                'a_second': [2, 3, 4, 5, 6, 7, 8, 9, 10, 11]
            }
        )
    )

    changed_ids = [0, 4, 6]
    changed_ids_df = cast(IndexDF, pd.DataFrame({'id': changed_ids}))
    not_changed_ids = [1, 2, 3, 5, 7, 8, 9]
    not_changed_ids_df = cast(IndexDF, pd.DataFrame({'id': not_changed_ids}))

    tbl2.store_chunk(
        pd.DataFrame(
            {
                'id': changed_ids,
                'a': [10, 10, 10]
            }
        )
    )

    BatchTransformIncStep(
        name='transform',
        input_dts=[tbl1, tbl2],
        output_dts=[tbl],
        func=inc_func
    ).run(ds)

    assert_df_equal(
        tbl.get_data(idx=changed_ids_df),
        pd.DataFrame(
            {
                'id': changed_ids,
                'a_first': [1, 5, 7],
                'a_second': [12, 12, 12]
            }
        )
    )

    assert_df_equal(
        tbl.get_data(idx=not_changed_ids_df),
        pd.DataFrame(
            {
                'id': not_changed_ids,
                'a_first': [2, 3, 4, 6, 8, 9, 10],
                'a_second': [3, 4, 5, 7, 9, 10, 11]
            }
        )
    )

    tbl1.store_chunk(
        pd.DataFrame(
            {
                'id': changed_ids,
                'a': [20, 20, 20]
            }
        )
    )

    BatchTransformIncStep(
        name='transform',
        input_dts=[tbl1, tbl2],
        output_dts=[tbl],
        func=inc_func
    ).run(ds)

    assert_df_equal(
        tbl.get_data(idx=changed_ids_df),
        pd.DataFrame(
            {
                'id': changed_ids,
                'a_first': [21, 21, 21],
                'a_second': [12, 12, 12]
            }
        )
    )

    assert_df_equal(
        tbl.get_data(idx=not_changed_ids_df),
        pd.DataFrame(
            {
                'id': not_changed_ids,
                'a_first': [2, 3, 4, 6, 8, 9, 10],
                'a_second': [3, 4, 5, 7, 9, 10, 11]
            }
        )
    )


def test_inc_process_many_several_outputs(dbconn) -> None:
    ds = DataStore(dbconn)

    bad_ids = [0, 1, 5, 8]
    good_ids = [2, 3, 4, 6, 7, 9]

    tbl = ds.create_table(
        'tbl',
        table_store=TableStoreDB(dbconn, 'tbl_data', TEST_SCHEMA, True)
    )
    tbl_good = ds.create_table(
        'tbl_good',
        table_store=TableStoreDB(dbconn, 'tbl_good_data', TEST_SCHEMA, True)
    )
    tbl_bad = ds.create_table(
        'tbl_bad',
        table_store=TableStoreDB(dbconn, 'tbl_bad_data', TEST_SCHEMA, True)
    )

    tbl.store_chunk(TEST_DF)

    def inc_func(df):
        df_good = df[df['id'].isin(good_ids)]
        df_bad = df[df['id'].isin(bad_ids)]
        return df_good, df_bad

    step = BatchTransformIncStep(
        name='transform',
        input_dts=[tbl],
        output_dts=[tbl_good, tbl_bad],
        func=inc_func
    )

    step.run(ds)
    assert_datatable_equal(tbl, TEST_DF)
    assert_datatable_equal(tbl_good, TEST_DF.loc[good_ids])
    assert_datatable_equal(tbl_bad, TEST_DF.loc[bad_ids])

    # Check this not delete the tables
    step.run(ds)
    assert_datatable_equal(tbl, TEST_DF)
    assert_datatable_equal(tbl_good, TEST_DF.loc[good_ids])
    assert_datatable_equal(tbl_bad, TEST_DF.loc[bad_ids])


def test_error_handling(dbconn) -> None:
    BAD_ID = 3
    GOOD_IDXS1 = [0, 1, 2, 3, 4, 5]
    CHUNKSIZE = 2

    ds = DataStore(dbconn)

    tbl = ds.create_table(
        'tbl',
        table_store=TableStoreDB(dbconn, 'tbl1_data', TEST_SCHEMA, True)
    )

    tbl_good = ds.create_table(
        'tbl_good',
        table_store=TableStoreDB(dbconn, 'tbl_good_data', TEST_SCHEMA, True)
    )

    def gen_bad1(chunksize: int = 1000):
        idx = TEST_DF.index

        for i in range(0, len(idx), chunksize):
            if i >= chunksize * 3:
                raise Exception("Test")

            yield TEST_DF.loc[idx[i:i+chunksize]]

    def gen_bad2(chunksize: int = 1000):
        idx = TEST_DF.index

        for i in range(0, len(idx), chunksize):
            if i >= chunksize * 2:
                raise Exception("Test")

            yield TEST_DF.loc[idx[i:i+chunksize]]

    # with pytest.raises(Exception):
    BatchGenerateStep(
        name='gen',
        output_dts=[tbl],
        func=gen_bad1,
        kwargs=dict(
            chunksize=CHUNKSIZE
        )
    ).run(ds)

    assert_datatable_equal(tbl, TEST_DF.loc[GOOD_IDXS1])

    def inc_func_bad(df):
        if BAD_ID in df['id'].values:
            raise Exception('TEST')
        return df

    def inc_func_good(df):
        return df

    BatchTransformIncStep(
        name='inc',
        input_dts=[tbl],
        output_dts=[tbl_good],
        func=inc_func_bad,
        chunk_size=1,
    ).run(ds)

    assert_datatable_equal(tbl_good, TEST_DF.loc[[0, 1, 2, 4, 5]])

    BatchTransformIncStep(
        name='inc',
        input_dts=[tbl],
        output_dts=[tbl_good],
        func=inc_func_good,
        chunk_size=CHUNKSIZE,
    ).run(ds)

    assert_datatable_equal(tbl_good, TEST_DF.loc[GOOD_IDXS1])

    # Checks that records are not being deleted
    # with pytest.raises(Exception):
    BatchGenerateStep(
        name='gen',
        output_dts=[tbl],
        func=gen_bad2,
        kwargs=dict(
            chunksize=CHUNKSIZE
        )
    ).run(ds)

    assert_datatable_equal(tbl, TEST_DF.loc[GOOD_IDXS1])

    BatchTransformIncStep(
        name='inc',
        input_dts=[tbl],
        output_dts=[tbl_good],
        func=inc_func_bad,
        kwargs=dict(
            chunksize=CHUNKSIZE
        )
    ).run(ds)

    assert_datatable_equal(tbl_good, TEST_DF.loc[GOOD_IDXS1])


def test_gen_from_empty_rows(dbconn) -> None:
    ds = DataStore(dbconn)
    tbl = ds.create_table('test', table_store=TableStoreDB(dbconn, 'tbl_data', TEST_SCHEMA, True))

    def proc_func():
        yield pd.DataFrame.from_records(
            {
                key: []
                for key in tbl.primary_keys
            }
        )

    # This should be ok
    BatchGenerateStep(
        name='gen',
        output_dts=[tbl],
        func=proc_func
    ).run(ds)
