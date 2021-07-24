import pytest

import cloudpickle
import pandas as pd
from sqlalchemy import Column, Numeric

from datapipe.store.database import TableStoreDB
from datapipe.datatable import DataTable, gen_process, gen_process_many, inc_process, inc_process_many
from datapipe.metastore import MetaStore

from .util import assert_df_equal, assert_idx_equal


TEST_SCHEMA = [
    Column('a', Numeric),
]

TEST_DF = pd.DataFrame(
    {
        'a': range(10)
    },
    index=[f'id_{i}' for i in range(10)]
)


TEST_DF_INC1 = TEST_DF.assign(a=lambda df: df['a'] + 1)
TEST_DF_INC2 = TEST_DF.assign(a=lambda df: df['a'] + 2)
TEST_DF_INC3 = TEST_DF.assign(a=lambda df: df['a'] + 3)


def yield_df(data):
    def f(*args, **kwargs):
        yield pd.DataFrame.from_records(data, columns=['id', 'a']).set_index('id')

    return f


def test_cloudpickle(dbconn) -> None:
    ms = MetaStore(dbconn)

    tbl = DataTable(
        'test',
        meta_table=ms.create_meta_table('test'),
        table_store=TableStoreDB(dbconn, 'test_data', TEST_SCHEMA, True)
    )

    cloudpickle.dumps([ms, tbl])


def test_simple(dbconn) -> None:
    ms = MetaStore(dbconn)

    tbl = DataTable(
        'test',
        meta_table=ms.create_meta_table('test'),
        table_store=TableStoreDB(dbconn, 'test_data', TEST_SCHEMA, True)
    )

    tbl.store(TEST_DF)


def test_store_less_values(dbconn) -> None:
    ms = MetaStore(dbconn)

    tbl = DataTable(
        'test',
        meta_table=ms.create_meta_table('test'),
        table_store=TableStoreDB(dbconn, 'test_data', TEST_SCHEMA, True))

    tbl.store(TEST_DF)
    assert_idx_equal(tbl.get_metadata().index, TEST_DF.index)

    tbl.store(TEST_DF[:5])
    assert_idx_equal(tbl.get_metadata().index, TEST_DF[:5].index)


def test_get_process_ids(dbconn) -> None:
    ms = MetaStore(dbconn)

    tbl1 = DataTable(
        'tbl1',
        meta_table=ms.create_meta_table('tbl1'),
        table_store=TableStoreDB(dbconn, 'tbl1_data', TEST_SCHEMA, True))
    tbl2 = DataTable(
        'tbl2',
        meta_table=ms.create_meta_table('tbl2'),
        table_store=TableStoreDB(dbconn, 'tbl2_data', TEST_SCHEMA, True))

    tbl1.store(TEST_DF)

    count, idx_dfs = ms.get_process_ids([tbl1.meta_table], [tbl2.meta_table])
    idx = pd.concat(list(idx_dfs))

    assert(sorted(list(idx.index)) == list(TEST_DF.index))

    tbl2.store(tbl1.get_data())

    upd_df = TEST_DF[:5].copy()
    upd_df['a'] += 1

    tbl1.store_chunk(upd_df)

    count, idx_dfs = ms.get_process_ids([tbl1.meta_table], [tbl2.meta_table])
    idx = pd.concat(list(idx_dfs))
    assert(sorted(list(idx.index)) == list(upd_df.index))


def test_gen_process(dbconn) -> None:
    ms = MetaStore(dbconn)

    tbl1_gen = DataTable(
        'tbl1_gen',
        meta_table=ms.create_meta_table('tbl1_gen'),
        table_store=TableStoreDB(dbconn, 'tbl1_gen_data', TEST_SCHEMA, True))
    tbl1 = DataTable(
        'tbl1',
        meta_table=ms.create_meta_table('tbl1'),
        table_store=TableStoreDB(dbconn, 'tbl1_data', TEST_SCHEMA, True))

    def gen():
        yield TEST_DF

    def func():
        return TEST_DF

    gen_process(
        tbl1_gen,
        gen
    )

    with pytest.raises(Exception):
        gen_process(
            tbl1,
            func
        )

    assert(assert_df_equal(tbl1_gen.get_data(), TEST_DF))

    def gen2():
        yield TEST_DF[:5]

    def func2():
        return TEST_DF[:5]

    gen_process(
        tbl1_gen,
        gen2
    )

    with pytest.raises(Exception):
        gen_process(
            tbl1,
            func2
        )

    assert(assert_df_equal(tbl1_gen.get_data(), TEST_DF[:5]))


def test_inc_process_modify_values(dbconn) -> None:
    ms = MetaStore(dbconn)

    tbl1 = DataTable(
        'tbl1',
        meta_table=ms.create_meta_table('tbl1'),
        table_store=TableStoreDB(dbconn, 'tbl1_data', TEST_SCHEMA, True))
    tbl2 = DataTable(
        'tbl2',
        meta_table=ms.create_meta_table('tbl2'),
        table_store=TableStoreDB(dbconn, 'tbl2_data', TEST_SCHEMA, True))

    def id_func(df):
        return df

    tbl1.store(TEST_DF)

    inc_process(ms, [tbl1], tbl2, id_func)

    assert(assert_df_equal(tbl2.get_data(), TEST_DF))

    ##########################
    tbl1.store(TEST_DF_INC1)

    inc_process(ms, [tbl1], tbl2, id_func)

    assert(assert_df_equal(tbl2.get_data(), TEST_DF_INC1))


def test_inc_process_delete_values_from_input(dbconn) -> None:
    ms = MetaStore(dbconn)

    tbl1 = DataTable(
        'tbl1',
        meta_table=ms.create_meta_table('tbl1'),
        table_store=TableStoreDB(dbconn, 'tbl1_data', TEST_SCHEMA, True))
    tbl2 = DataTable(
        'tbl2',
        meta_table=ms.create_meta_table('tbl2'),
        table_store=TableStoreDB(dbconn, 'tbl2_data', TEST_SCHEMA, True))

    def id_func(df):
        return df

    tbl1.store(TEST_DF)

    inc_process(ms, [tbl1], tbl2, id_func)

    assert(assert_df_equal(tbl2.get_data(), TEST_DF))

    ##########################
    tbl1.store(TEST_DF[:5])

    inc_process(ms, [tbl1], tbl2, id_func, chunksize=2)

    assert(assert_df_equal(tbl2.get_data(), TEST_DF[:5]))


def test_inc_process_delete_values_from_proc(dbconn) -> None:
    ms = MetaStore(dbconn)

    tbl1 = DataTable(
        'tbl1',
        meta_table=ms.create_meta_table('tbl1'),
        table_store=TableStoreDB(dbconn, 'tbl1_data', TEST_SCHEMA, True))
    tbl2 = DataTable(
        'tbl2',
        meta_table=ms.create_meta_table('tbl2'),
        table_store=TableStoreDB(dbconn, 'tbl2_data', TEST_SCHEMA, True))

    def id_func(df):
        return df[:5]

    tbl2.store(TEST_DF)

    tbl1.store(TEST_DF)

    inc_process(ms, [tbl1], tbl2, id_func)

    assert(assert_df_equal(tbl2.get_data(), TEST_DF[:5]))


def test_inc_process_proc_no_change(dbconn) -> None:
    ms = MetaStore(dbconn)

    tbl1 = DataTable(
        'tbl1',
        meta_table=ms.create_meta_table('tbl1'),
        table_store=TableStoreDB(dbconn, 'tbl1_data', TEST_SCHEMA, True))
    tbl2 = DataTable(
        'tbl2',
        meta_table=ms.create_meta_table('tbl2'),
        table_store=TableStoreDB(dbconn, 'tbl2_data', TEST_SCHEMA, True))

    def id_func(df):
        return TEST_DF

    tbl2.store(TEST_DF)
    tbl1.store(TEST_DF)

    count, idx_dfs = ms.get_process_ids([tbl1.meta_table], [tbl2.meta_table])
    idx = pd.concat(list(idx_dfs))

    assert(len(idx) == len(TEST_DF))

    inc_process(ms, [tbl1], tbl2, id_func)

    count, idx_gen = ms.get_process_ids([tbl1.meta_table], [tbl2.meta_table])
    idx_dfs = list(idx_gen)
    idx = pd.concat(idx_dfs) if len(idx_dfs) > 0 else []

    assert(len(idx) == 0)

    tbl1.store(TEST_DF_INC1)

    count, idx_dfs = ms.get_process_ids([tbl1.meta_table], [tbl2.meta_table])
    idx = pd.concat(list(idx_dfs))

    assert(len(idx) == len(TEST_DF))

    inc_process(ms, [tbl1], tbl2, id_func)

    count, idx_gen = ms.get_process_ids([tbl1.meta_table], [tbl2.meta_table])
    idx_dfs = list(idx_gen)
    idx = pd.concat(idx_dfs) if len(idx_dfs) > 0 else []

    assert(len(idx) == 0)

# TODO тест inc_process 2->1
# TODO тест inc_process 2->1, удаление строки, 2->1


def test_gen_process_many(dbconn) -> None:
    ms = MetaStore(dbconn)

    tbl_gen = DataTable(
        'tbl_gen',
        meta_table=ms.create_meta_table('tbl_gen'),
        table_store=TableStoreDB(dbconn, 'tbl_gen_data', TEST_SCHEMA, True))
    tbl1_gen = DataTable(
        'tbl1_gen',
        meta_table=ms.create_meta_table('tbl1_gen'),
        table_store=TableStoreDB(dbconn, 'tbl1_gen_data', TEST_SCHEMA, True))
    tbl2_gen = DataTable(
        'tbl2_gen',
        meta_table=ms.create_meta_table('tbl2_gen'),
        table_store=TableStoreDB(dbconn, 'tbl2_gen_data', TEST_SCHEMA, True))
    tbl3_gen = DataTable(
        'tbl3_gen',
        meta_table=ms.create_meta_table('tbl3_gen'),
        table_store=TableStoreDB(dbconn, 'tbl3_gen_data', TEST_SCHEMA, True))
    tbl = DataTable(
        'tbl',
        meta_table=ms.create_meta_table('tbl'),
        table_store=TableStoreDB(dbconn, 'tbl_data', TEST_SCHEMA, True))
    tbl1 = DataTable(
        'tbl1',
        meta_table=ms.create_meta_table('tbl1'),
        table_store=TableStoreDB(dbconn, 'tbl1_data', TEST_SCHEMA, True))
    tbl2 = DataTable(
        'tbl2',
        meta_table=ms.create_meta_table('tbl2'),
        table_store=TableStoreDB(dbconn, 'tbl2_data', TEST_SCHEMA, True))
    tbl3 = DataTable(
        'tbl3',
        meta_table=ms.create_meta_table('tbl3'),
        table_store=TableStoreDB(dbconn, 'tbl3_data', TEST_SCHEMA, True))

    def gen():
        yield (TEST_DF, TEST_DF_INC1, TEST_DF_INC2, TEST_DF_INC3)

    def func():
        return TEST_DF, TEST_DF_INC1, TEST_DF_INC2, TEST_DF_INC3

    gen_process_many(
        [tbl_gen, tbl1_gen, tbl2_gen, tbl3_gen],
        gen
    )

    with pytest.raises(Exception):
        gen_process_many(
            [tbl, tbl1, tbl2, tbl3],
            func
        )

    assert(assert_df_equal(tbl_gen.get_data(), TEST_DF))
    assert(assert_df_equal(tbl1_gen.get_data(), TEST_DF_INC1))
    assert(assert_df_equal(tbl2_gen.get_data(), TEST_DF_INC2))
    assert(assert_df_equal(tbl3_gen.get_data(), TEST_DF_INC3))


def test_inc_process_many_modify_values(dbconn) -> None:
    ms = MetaStore(dbconn)

    tbl = DataTable(
        'tbl',
        meta_table=ms.create_meta_table('tbl'),
        table_store=TableStoreDB(dbconn, 'tbl_data', TEST_SCHEMA, True))
    tbl1 = DataTable(
        'tbl1',
        meta_table=ms.create_meta_table('tbl1'),
        table_store=TableStoreDB(dbconn, 'tbl1_data', TEST_SCHEMA, True))
    tbl2 = DataTable(
        'tbl2',
        meta_table=ms.create_meta_table('tbl2'),
        table_store=TableStoreDB(dbconn, 'tbl2_data', TEST_SCHEMA, True))
    tbl3 = DataTable(
        'tbl3',
        meta_table=ms.create_meta_table('tbl3'),
        table_store=TableStoreDB(dbconn, 'tbl3_data', TEST_SCHEMA, True))

    def inc_func(df):
        df1 = df.copy()
        df2 = df.copy()
        df3 = df.copy()
        df1['a'] += 1
        df2['a'] += 2
        df3['a'] += 3
        return df1, df2, df3

    tbl.store(TEST_DF)

    inc_process_many(ms, [tbl], [tbl1, tbl2, tbl3], inc_func)

    assert(assert_df_equal(tbl1.get_data(), TEST_DF_INC1))
    assert(assert_df_equal(tbl2.get_data(), TEST_DF_INC2))
    assert(assert_df_equal(tbl3.get_data(), TEST_DF_INC3))

    ##########################
    tbl.store(TEST_DF[:5])

    def inc_func_inv(df):
        df1 = df.copy()
        df2 = df.copy()
        df3 = df.copy()
        df1['a'] += 1
        df2['a'] += 2
        df3['a'] += 3
        return df3, df2, df1

    inc_process_many(ms, [tbl], [tbl3, tbl2, tbl1], inc_func_inv)

    assert(assert_df_equal(tbl1.get_data(), TEST_DF_INC1[:5]))
    assert(assert_df_equal(tbl2.get_data(), TEST_DF_INC2[:5]))
    assert(assert_df_equal(tbl3.get_data(), TEST_DF_INC3[:5]))

    ##########################

    tbl.store_chunk(TEST_DF[5:])

    inc_process_many(ms, [tbl], [tbl1, tbl2, tbl3], inc_func)
    assert(assert_df_equal(tbl1.get_data(), TEST_DF_INC1))
    assert(assert_df_equal(tbl2.get_data(), TEST_DF_INC2))
    assert(assert_df_equal(tbl3.get_data(), TEST_DF_INC3))


def test_inc_process_many_several_inputs(dbconn) -> None:
    ms = MetaStore(dbconn)

    tbl = DataTable(
        'tbl',
        meta_table=ms.create_meta_table('tbl'),
        table_store=TableStoreDB(
            dbconn, 'tbl_data', [Column('a_first', Numeric), Column('a_second', Numeric)], True
        )
    )
    tbl1 = DataTable(
        'tbl1',
        meta_table=ms.create_meta_table('tbl1'),
        table_store=TableStoreDB(dbconn, 'tbl1_data', TEST_SCHEMA, True))
    tbl2 = DataTable(
        'tbl2',
        meta_table=ms.create_meta_table('tbl2'),
        table_store=TableStoreDB(dbconn, 'tbl2_data', TEST_SCHEMA, True))

    def inc_func(df1, df2):
        df = pd.merge(
            left=df1,
            right=df2,
            left_index=True,
            right_index=True,
            suffixes=('_first', '_second')
        )
        df['a_first'] += 1
        df['a_second'] += 2
        return df

    tbl1.store(TEST_DF)
    tbl2.store(TEST_DF)

    inc_process_many(ms, [tbl1, tbl2], [tbl], inc_func)
    assert(
        assert_df_equal(
            tbl.get_data(),
            pd.DataFrame(
                {
                    'a_first': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
                    'a_second': [2, 3, 4, 5, 6, 7, 8, 9, 10, 11]
                },
                index=[f'id_{i}' for i in range(10)]
            )
        )
    )

    changed_indexes = [f'id_{i}' for i in [0, 4, 6]]
    not_changed_indexes = [f'id_{i}' for i in [1, 2, 3, 5, 7, 8, 9]]

    tbl2.store_chunk(
        pd.DataFrame(
            {
                'a': [10, 10, 10]
            },
            index=changed_indexes
        )
    )

    inc_process_many(ms, [tbl1, tbl2], [tbl], inc_func)

    assert(
        assert_df_equal(
            tbl.get_data(idx=changed_indexes),
            pd.DataFrame(
                {
                    'a_first': [1, 5, 7],
                    'a_second': [12, 12, 12]
                },
                index=changed_indexes
            )
        )
    )
    assert(
        assert_df_equal(
            tbl.get_data(idx=not_changed_indexes),
            pd.DataFrame(
                {
                    'a_first': [2, 3, 4, 6, 8, 9, 10],
                    'a_second': [3, 4, 5, 7, 9, 10, 11]
                },
                index=not_changed_indexes
            )
        )
    )

    tbl1.store_chunk(
        pd.DataFrame(
            {
                'a': [20, 20, 20]
            },
            index=changed_indexes
        )
    )

    inc_process_many(ms, [tbl1, tbl2], [tbl], inc_func)

    assert(
        assert_df_equal(
            tbl.get_data(idx=changed_indexes),
            pd.DataFrame(
                {
                    'a_first': [21, 21, 21],
                    'a_second': [12, 12, 12]
                },
                index=changed_indexes
            )
        )
    )
    assert(
        assert_df_equal(
            tbl.get_data(idx=not_changed_indexes),
            pd.DataFrame(
                {
                    'a_first': [2, 3, 4, 6, 8, 9, 10],
                    'a_second': [3, 4, 5, 7, 9, 10, 11]
                },
                index=not_changed_indexes
            )
        )
    )


def test_inc_process_many_several_outputs(dbconn) -> None:
    ms = MetaStore(dbconn)

    BAD_IDXS = ['id_0', 'id_1', 'id_5', 'id_8']
    GOOD_IDXS = ['id_2', 'id_3', 'id_4', 'id_6', 'id_7', 'id_9']

    tbl = DataTable(
        'tbl',
        meta_table=ms.create_meta_table('tbl'),
        table_store=TableStoreDB(dbconn, 'tbl_data', TEST_SCHEMA, True))
    tbl_good = DataTable(
        'tbl_good',
        meta_table=ms.create_meta_table('tbl_good'),
        table_store=TableStoreDB(dbconn, 'tbl_good_data', TEST_SCHEMA, True))
    tbl_bad = DataTable(
        'tbl_bad',
        meta_table=ms.create_meta_table('tbl_bad'),
        table_store=TableStoreDB(dbconn, 'tbl_bad_data', TEST_SCHEMA, True))

    tbl.store(TEST_DF)

    def inc_func(df):
        df_good = df.drop(index=BAD_IDXS)
        df_bad = df.drop(index=GOOD_IDXS)
        return df_good, df_bad

    inc_process_many(ms, [tbl], [tbl_good, tbl_bad], inc_func)
    assert(assert_df_equal(tbl.get_data(), TEST_DF))
    assert(assert_df_equal(tbl_good.get_data(), TEST_DF.loc[GOOD_IDXS]))
    assert(assert_df_equal(tbl_bad.get_data(), TEST_DF.loc[BAD_IDXS]))

    # Check this not delete the tables
    inc_process_many(ms, [tbl], [tbl_good, tbl_bad], inc_func)
    assert(assert_df_equal(tbl.get_data(), TEST_DF))
    assert(assert_df_equal(tbl_good.get_data(), TEST_DF.loc[GOOD_IDXS]))
    assert(assert_df_equal(tbl_bad.get_data(), TEST_DF.loc[BAD_IDXS]))


def test_error_handling(dbconn) -> None:
    BAD_ID = 'id_3'
    GOOD_IDXS1 = ['id_0', 'id_1', 'id_2', 'id_3', 'id_4', 'id_5']
    GOOD_IDXS2 = ['id_0', 'id_1', 'id_4', 'id_5']
    CHUNKSIZE = 2

    ms = MetaStore(dbconn)

    tbl = DataTable(
        'tbl',
        meta_table=ms.create_meta_table('tbl'),
        table_store=TableStoreDB(dbconn, 'tbl1_data', TEST_SCHEMA, True))

    tbl_good = DataTable(
        'tbl_good',
        meta_table=ms.create_meta_table('tbl_good'),
        table_store=TableStoreDB(dbconn, 'tbl_good_data', TEST_SCHEMA, True))

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

    gen_process_many(
        dts=[tbl],
        proc_func=gen_bad1,
        chunksize=CHUNKSIZE
    )

    assert(assert_df_equal(tbl.get_data(), TEST_DF.loc[GOOD_IDXS1]))

    def inc_func_bad(df):
        if BAD_ID in df.index:
            raise Exception('TEST')
        return df

    def inc_func_good(df):
        return df

    inc_process_many(
        ms,
        [tbl],
        [tbl_good],
        inc_func_bad,
        chunksize=CHUNKSIZE
    )

    assert(assert_df_equal(tbl_good.get_data(), TEST_DF.loc[GOOD_IDXS2]))

    inc_process_many(
        ms,
        [tbl],
        [tbl_good],
        inc_func_good,
        chunksize=CHUNKSIZE
    )

    assert(assert_df_equal(tbl_good.get_data().sort_index(), TEST_DF.loc[GOOD_IDXS1]))
    # Checks that records are not being deleted
    gen_process_many(
        dts=[tbl],
        proc_func=gen_bad2,
        chunksize=CHUNKSIZE
    )

    assert(assert_df_equal(tbl.get_data().sort_index(), TEST_DF.loc[GOOD_IDXS1]))

    inc_process_many(
        ms,
        [tbl],
        [tbl_good],
        inc_func_bad,
        chunksize=CHUNKSIZE
    )

    assert(assert_df_equal(tbl_good.get_data().sort_index(), TEST_DF.loc[GOOD_IDXS1]))
