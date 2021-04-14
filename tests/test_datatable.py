import time
import tempfile
from pathlib import Path
from typing import Tuple

import pytest
import os

import cloudpickle
import pandas as pd
from sqlalchemy import create_engine, Column, Numeric

from datapipe.store.table_store_sql import TableStoreDB
from datapipe.datatable import DataTable, gen_process, gen_process_many, inc_process, inc_process_many
from datapipe.metastore import DBConn, MetaStore, PRIMARY_KEY
from c12n_pipe.io.data_catalog import DBTable, DataCatalog
from c12n_pipe.io.node import Pipeline, StoreNode, PythonNode, LabelStudioNode
from sqlalchemy.sql.sqltypes import JSON

from tests.util import assert_df_equal, assert_idx_equal


# DBCONNSTR = f'postgresql://postgres:password@{os.getenv("POSTGRES_HOST", "localhost")}:{os.getenv("POSTGRES_PORT", 5432)}/postgres'
DBCONNSTR = 'sqlite:///:memory:'
DB_TEST_SCHEMA = None
DB_CREATE_SCHEMA = False

TEST_SCHEMA = PRIMARY_KEY + [
    Column('a', Numeric),
]

TEST_DF = pd.DataFrame(
    {
        'a': range(10)
    },
    index=[f'id_{i}' for i in range(10)]
)


TEST_DF_INC1 = TEST_DF.assign(a = lambda df: df['a'] + 1)
TEST_DF_INC2 = TEST_DF.assign(a = lambda df: df['a'] + 2)
TEST_DF_INC3 = TEST_DF.assign(a = lambda df: df['a'] + 3)


def yield_df(data):
    def f(*args, **kwargs):
        yield pd.DataFrame.from_records(data, columns=['id', 'a']).set_index('id')

    return f


@pytest.fixture
def dbconn():
    if DB_CREATE_SCHEMA:
        eng = create_engine(DBCONNSTR)

        try:
            eng.execute(f'DROP SCHEMA {DB_TEST_SCHEMA} CASCADE')
        except:
            pass

        eng.execute(f'CREATE SCHEMA {DB_TEST_SCHEMA}')

        yield DBConn(DBCONNSTR, DB_TEST_SCHEMA)

        eng.execute(f'DROP SCHEMA {DB_TEST_SCHEMA} CASCADE')
    
    else:
        yield DBConn(DBCONNSTR, DB_TEST_SCHEMA)

def test_cloudpickle(dbconn) -> None:
    ds = MetaStore(dbconn)

    tbl = DataTable(ds, 'test', table_store=TableStoreDB(dbconn, 'test_data', TEST_SCHEMA, True))

    cloudpickle.dumps([ds, tbl])


def test_simple(dbconn) -> None:
    ds = MetaStore(dbconn)

    tbl = DataTable(ds, 'test', table_store=TableStoreDB(dbconn, 'test_data', TEST_SCHEMA, True))

    tbl.store(TEST_DF)


def test_store_less_values(dbconn) -> None:
    ds = MetaStore(dbconn)

    tbl = DataTable(ds, 'test', table_store=TableStoreDB(dbconn, 'test_data', TEST_SCHEMA, True))

    tbl.store(TEST_DF)
    assert_idx_equal(tbl.get_metadata().index, TEST_DF.index)

    tbl.store(TEST_DF[:5])
    assert_idx_equal(tbl.get_metadata().index, TEST_DF[:5].index)


def test_get_process_ids(dbconn) -> None:
    ds = MetaStore(dbconn)

    tbl1 = DataTable(ds, 'tbl1', table_store=TableStoreDB(dbconn, 'tbl1_data', TEST_SCHEMA, True))
    tbl2 = DataTable(ds, 'tbl2', table_store=TableStoreDB(dbconn, 'tbl2_data', TEST_SCHEMA, True))

    tbl1.store(TEST_DF)

    idx = ds.get_process_ids([tbl1], [tbl2])
    assert(list(idx) == list(TEST_DF.index))

    tbl2.store(tbl1.get_data())

    upd_df = TEST_DF[:5].copy()
    upd_df['a'] += 1

    tbl1.store_chunk(upd_df)

    idx = ds.get_process_ids([tbl1], [tbl2])
    assert(list(idx) == list(upd_df.index))


def test_gen_process(dbconn) -> None:
    ds = MetaStore(dbconn)

    tbl1_gen = DataTable(ds, 'tbl1_gen', table_store=TableStoreDB(dbconn, 'tbl1_gen_data', TEST_SCHEMA, True))
    tbl1 = DataTable(ds, 'tbl1', table_store=TableStoreDB(dbconn, 'tbl1_data', TEST_SCHEMA, True))

    def gen():
        yield TEST_DF

    def func():
        return TEST_DF

    gen_process(
        tbl1_gen,
        gen
    )

    gen_process(
        tbl1,
        func
    )

    assert(assert_df_equal(tbl1_gen.get_data(), TEST_DF))
    assert(assert_df_equal(tbl1.get_data(), TEST_DF))

    def gen2():
        yield TEST_DF[:5]

    def func2():
        return TEST_DF[:5]

    gen_process(
        tbl1_gen,
        gen2
    )

    gen_process(
        tbl1,
        func2
    )

    assert(assert_df_equal(tbl1_gen.get_data(), TEST_DF[:5]))
    assert(assert_df_equal(tbl1.get_data(), TEST_DF[:5]))


def test_inc_process_modify_values(dbconn) -> None:
    ds = MetaStore(dbconn)

    tbl1 = DataTable(ds, 'tbl1', table_store=TableStoreDB(dbconn, 'tbl1_data', TEST_SCHEMA, True))
    tbl2 = DataTable(ds, 'tbl2', table_store=TableStoreDB(dbconn, 'tbl2_data', TEST_SCHEMA, True))

    def id_func(df):
        return df

    tbl1.store(TEST_DF)

    inc_process(ds, [tbl1], tbl2, id_func)

    assert(assert_df_equal(tbl2.get_data(), TEST_DF))

    ##########################
    tbl1.store(TEST_DF_INC1)

    inc_process(ds, [tbl1], tbl2, id_func)

    assert(assert_df_equal(tbl2.get_data(), TEST_DF_INC1))


def test_inc_process_delete_values_from_input(dbconn) -> None:
    ds = MetaStore(dbconn)

    tbl1 = DataTable(ds, 'tbl1', table_store=TableStoreDB(dbconn, 'tbl1_data', TEST_SCHEMA, True))
    tbl2 = DataTable(ds, 'tbl2', table_store=TableStoreDB(dbconn, 'tbl2_data', TEST_SCHEMA, True))

    def id_func(df):
        return df

    tbl1.store(TEST_DF)

    inc_process(ds, [tbl1], tbl2, id_func)

    assert(assert_df_equal(tbl2.get_data(), TEST_DF))

    ##########################
    tbl1.store(TEST_DF[:5])

    inc_process(ds, [tbl1], tbl2, id_func, chunksize=2)

    assert(assert_df_equal(tbl2.get_data(), TEST_DF[:5]))


def test_inc_process_delete_values_from_proc(dbconn) -> None:
    ds = MetaStore(dbconn)

    tbl1 = DataTable(ds, 'tbl1', table_store=TableStoreDB(dbconn, 'tbl1_data', TEST_SCHEMA, True))
    tbl2 = DataTable(ds, 'tbl2', table_store=TableStoreDB(dbconn, 'tbl2_data', TEST_SCHEMA, True))

    def id_func(df):
        return df[:5]

    tbl2.store(TEST_DF)

    tbl1.store(TEST_DF)

    inc_process(ds, [tbl1], tbl2, id_func)

    assert(assert_df_equal(tbl2.get_data(), TEST_DF[:5]))


def test_inc_process_proc_no_change(dbconn) -> None:
    ds = MetaStore(dbconn)

    tbl1 = DataTable(ds, 'tbl1', table_store=TableStoreDB(dbconn, 'tbl1_data', TEST_SCHEMA, True))
    tbl2 = DataTable(ds, 'tbl2', table_store=TableStoreDB(dbconn, 'tbl2_data', TEST_SCHEMA, True))

    def id_func(df):
        return TEST_DF

    tbl2.store(TEST_DF)
    tbl1.store(TEST_DF)

    assert(len(ds.get_process_ids([tbl1], [tbl2])) == len(TEST_DF))

    inc_process(ds, [tbl1], tbl2, id_func)

    assert(len(ds.get_process_ids([tbl1], [tbl2])) == 0)

    tbl1.store(TEST_DF_INC1)

    assert(len(ds.get_process_ids([tbl1], [tbl2])) == len(TEST_DF))

    inc_process(ds, [tbl1], tbl2, id_func)

    assert(len(ds.get_process_ids([tbl1], [tbl2])) == 0)

# TODO тест inc_process 2->1
# TODO тест inc_process 2->1, удаление строки, 2->1


def test_gen_process_many(dbconn) -> None:
    ds = MetaStore(dbconn)

    tbl_gen = DataTable(ds, 'tbl_gen', table_store=TableStoreDB(dbconn, 'tbl_gen_data',TEST_SCHEMA, True))
    tbl1_gen = DataTable(ds, 'tbl1_gen', table_store=TableStoreDB(dbconn, 'tbl1_gen_data',TEST_SCHEMA, True))
    tbl2_gen = DataTable(ds, 'tbl2_gen', table_store=TableStoreDB(dbconn, 'tbl2_gen_data',TEST_SCHEMA, True))
    tbl3_gen = DataTable(ds, 'tbl3_gen', table_store=TableStoreDB(dbconn, 'tbl3_gen_data',TEST_SCHEMA, True))
    tbl = DataTable(ds, 'tbl', table_store=TableStoreDB(dbconn, 'tbl_data',TEST_SCHEMA, True))
    tbl1 = DataTable(ds, 'tbl1', table_store=TableStoreDB(dbconn, 'tbl1_data',TEST_SCHEMA, True))
    tbl2 = DataTable(ds, 'tbl2', table_store=TableStoreDB(dbconn, 'tbl2_data',TEST_SCHEMA, True))
    tbl3 = DataTable(ds, 'tbl3', table_store=TableStoreDB(dbconn, 'tbl3_data',TEST_SCHEMA, True))

    def gen():
        yield (TEST_DF, TEST_DF_INC1, TEST_DF_INC2, TEST_DF_INC3)

    def func():
        return TEST_DF, TEST_DF_INC1, TEST_DF_INC2, TEST_DF_INC3

    gen_process_many(
        [tbl_gen, tbl1_gen, tbl2_gen, tbl3_gen],
        gen
    )

    gen_process_many(
        [tbl, tbl1, tbl2, tbl3],
        func
    )

    assert(assert_df_equal(tbl_gen.get_data(), TEST_DF))
    assert(assert_df_equal(tbl1_gen.get_data(), TEST_DF_INC1))
    assert(assert_df_equal(tbl2_gen.get_data(), TEST_DF_INC2))
    assert(assert_df_equal(tbl3_gen.get_data(), TEST_DF_INC3))
    assert(assert_df_equal(tbl.get_data(), TEST_DF))
    assert(assert_df_equal(tbl1.get_data(), TEST_DF_INC1))
    assert(assert_df_equal(tbl2.get_data(), TEST_DF_INC2))
    assert(assert_df_equal(tbl3.get_data(), TEST_DF_INC3))


def test_inc_process_many_modify_values(dbconn) -> None:
    ds = MetaStore(dbconn)

    tbl = DataTable(ds, 'tbl', table_store=TableStoreDB(dbconn, 'tbl_data', TEST_SCHEMA, True))
    tbl1 = DataTable(ds, 'tbl1', table_store=TableStoreDB(dbconn, 'tbl1_data', TEST_SCHEMA, True))
    tbl2 = DataTable(ds, 'tbl2', table_store=TableStoreDB(dbconn, 'tbl2_data', TEST_SCHEMA, True))
    tbl3 = DataTable(ds, 'tbl3', table_store=TableStoreDB(dbconn, 'tbl3_data', TEST_SCHEMA, True))

    def inc_func(df):
        df1 = df.copy()
        df2 = df.copy()
        df3 = df.copy()
        df1['a'] += 1
        df2['a'] += 2
        df3['a'] += 3
        return df1, df2, df3

    tbl.store(TEST_DF)

    inc_process_many(ds, [tbl], [tbl1, tbl2, tbl3], inc_func)

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

    inc_process_many(ds, [tbl], [tbl3, tbl2, tbl1], inc_func_inv)

    assert(assert_df_equal(tbl1.get_data(), TEST_DF_INC1[:5]))
    assert(assert_df_equal(tbl2.get_data(), TEST_DF_INC2[:5]))
    assert(assert_df_equal(tbl3.get_data(), TEST_DF_INC3[:5]))

    ##########################

    tbl.store_chunk(TEST_DF[5:])

    inc_process_many(ds, [tbl], [tbl1, tbl2, tbl3], inc_func)
    assert(assert_df_equal(tbl1.get_data(), TEST_DF_INC1))
    assert(assert_df_equal(tbl2.get_data(), TEST_DF_INC2))
    assert(assert_df_equal(tbl3.get_data(), TEST_DF_INC3))


def test_inc_process_many_several_outputs(dbconn) -> None:
    ds = MetaStore(dbconn)

    BAD_IDXS = ['id_0', 'id_1', 'id_5', 'id_8']
    GOOD_IDXS = ['id_2', 'id_3', 'id_4', 'id_6', 'id_7', 'id_9']

    tbl = DataTable(ds, 'tbl', table_store=TableStoreDB(dbconn, 'tbl_data', TEST_SCHEMA, True))
    tbl_good = DataTable(ds, 'tbl_good', table_store=TableStoreDB(dbconn, 'tbl_good_data', TEST_SCHEMA, True))
    tbl_bad = DataTable(ds, 'tbl_bad', table_store=TableStoreDB(dbconn, 'tbl_bad_data', TEST_SCHEMA, True))

    tbl.store(TEST_DF)

    def inc_func(df):
        df_good = df.drop(index=BAD_IDXS)
        df_bad = df.drop(index=GOOD_IDXS)
        return df_good, df_bad

    inc_process_many(ds, [tbl], [tbl_good, tbl_bad], inc_func)
    assert(assert_df_equal(tbl.get_data(), TEST_DF))
    assert(assert_df_equal(tbl_good.get_data(), TEST_DF.loc[GOOD_IDXS]))
    assert(assert_df_equal(tbl_bad.get_data(), TEST_DF.loc[BAD_IDXS]))

    # Check this not delete the tables
    inc_process_many(ds, [tbl], [tbl_good, tbl_bad], inc_func)
    assert(assert_df_equal(tbl.get_data(), TEST_DF))
    assert(assert_df_equal(tbl_good.get_data(), TEST_DF.loc[GOOD_IDXS]))
    assert(assert_df_equal(tbl_bad.get_data(), TEST_DF.loc[BAD_IDXS]))


def test_data_catalog(dbconn) -> None:
    ds = MetaStore(dbconn)
    data_catalog = DataCatalog(
        ds=ds,
        catalog={
            'test_df': DBTable([Column('a', Numeric)])
        },
    )

    tbl = data_catalog.get_data_table('test_df')
    tbl.store(TEST_DF)
    assert(assert_df_equal(tbl.get_data(), TEST_DF))


def test_store_node(dbconn) -> None:
    ds = MetaStore(dbconn)
    data_catalog = DataCatalog(
        ds=ds,
        catalog={
            'test_df': DBTable([Column('a', Numeric)])
        },
    )

    def proc_func():
        return TEST_DF

    store_node = StoreNode(
        proc_func=proc_func,
        outputs=['test_df']
    )
    store_node.run(data_catalog=data_catalog)

    tbl = data_catalog.get_data_table('test_df')
    assert(assert_df_equal(tbl.get_data(), TEST_DF))


def test_python_node(dbconn) -> None:
    ds = MetaStore(dbconn)
    data_catalog = DataCatalog(
        ds=ds,
        catalog={
            'test_df': DBTable([Column('a', Numeric)]),
            'test_df_inc1': DBTable([Column('a', Numeric)]),
            'test_df_inc2': DBTable([Column('a', Numeric)]),
            'test_df_inc3': DBTable([Column('a', Numeric)])
        },
    )
    data_catalog.get_data_table('test_df').store(TEST_DF)

    def proc_func(df):
        df1 = df.copy()
        df2 = df.copy()
        df3 = df.copy()
        df1['a'] += 1
        df2['a'] += 2
        df3['a'] += 3
        return df1, df2, df3

    python_node = PythonNode(
        proc_func=proc_func,
        inputs=['test_df'],
        outputs=['test_df_inc1', 'test_df_inc2', 'test_df_inc3']
    )
    python_node.run(data_catalog=data_catalog)

    tbl = data_catalog.get_data_table('test_df')
    tbl1 = data_catalog.get_data_table('test_df_inc1')
    tbl2 = data_catalog.get_data_table('test_df_inc2')
    tbl3 = data_catalog.get_data_table('test_df_inc3')

    assert(assert_df_equal(tbl.get_data(), TEST_DF))
    assert(assert_df_equal(tbl1.get_data(), TEST_DF_INC1))
    assert(assert_df_equal(tbl2.get_data(), TEST_DF_INC2))
    assert(assert_df_equal(tbl3.get_data(), TEST_DF_INC3))


@pytest.fixture
def temp_dir():
    old_cwd = Path.cwd()
    with tempfile.TemporaryDirectory() as tmp_dir_path:
        os.chdir(tmp_dir_path)
        yield 'ok'
        os.chdir(old_cwd)


@pytest.mark.usefixtures('temp_dir')
def test_label_studio_node(dbconn) -> None:
    ds = MetaStore(dbconn)
    data_catalog = DataCatalog(
        ds=ds,
        catalog={
            'data_input': DBTable([Column('data', JSON)]),
            'data_annotation': DBTable([Column('data', JSON)])
        },
    )

    label_studio_node = LabelStudioNode(
        project_path='ls_project_test/',
        input='data_input',
        output='data_annotation',
        port=8080
    )
    label_studio_node.run(data_catalog=data_catalog)
    time.sleep(2)
    label_studio_node.terminate_services()


def test_pipeline(dbconn) -> None:
    ds = MetaStore(dbconn)
    data_catalog = DataCatalog(
        ds=ds,
        catalog={
            'test_df': DBTable([Column('a', Numeric)]),
            'test_df_inc1': DBTable([Column('a', Numeric)]),
            'test_df_inc2': DBTable([Column('a', Numeric)]),
            'test_df_inc3': DBTable([Column('a', Numeric)])
        },
    )

    def store_func():
        return TEST_DF

    def inc_func(df):
        df['a'] += 1
        return df

    def inc2_func(df1, df2):
        df1['a'] += 2
        df2['a'] += 2
        return df1, df2

    pipeline = Pipeline(
        data_catalog=data_catalog,
        pipeline=[
            StoreNode(
                proc_func=store_func,
                outputs=['test_df']
            ),
            PythonNode(
                proc_func=inc_func,
                inputs=['test_df'],
                outputs=['test_df_inc1']
            ),
            PythonNode(
                proc_func=inc2_func,
                inputs=['test_df', 'test_df_inc1'],
                outputs=['test_df_inc2', 'test_df_inc3']
            )
        ]
    )

    pipeline.run()

    tbl = data_catalog.get_data_table('test_df')
    tbl1 = data_catalog.get_data_table('test_df_inc1')
    tbl2 = data_catalog.get_data_table('test_df_inc2')
    tbl3 = data_catalog.get_data_table('test_df_inc3')

    assert(assert_df_equal(tbl.get_data(), TEST_DF))
    assert(assert_df_equal(tbl1.get_data(), TEST_DF_INC1))
    assert(assert_df_equal(tbl2.get_data(), TEST_DF_INC2))
    assert(assert_df_equal(tbl3.get_data(), TEST_DF_INC3))
