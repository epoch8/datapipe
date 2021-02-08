import time
import tempfile
from pathlib import Path
from typing import Tuple

import pytest
import os

import cloudpickle
import pandas as pd
from sqlalchemy import create_engine, Column, Numeric

from c12n_pipe.datatable import DataStore, gen_process, gen_process_many, inc_process, inc_process_many
from c12n_pipe.io.data_catalog import AbstractDataTable, DataCatalog
from c12n_pipe.io.node import Pipeline, StoreNode, PythonNode
from c12n_pipe.label_studio_utils.label_studio_node import LabelStudioNode
from sqlalchemy.sql.sqltypes import JSON

DBCONNSTR = f'postgresql://postgres:password@{os.getenv("POSTGRES_HOST", "localhost")}:{os.getenv("POSTGRES_PORT", 5432)}/postgres'

TEST_SCHEMA = [
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
def test_schema():
    eng = create_engine(DBCONNSTR)

    try:
        eng.execute('DROP SCHEMA test CASCADE')
    except:
        pass

    eng.execute('CREATE SCHEMA test')

    yield 'ok'

    eng.execute('DROP SCHEMA test CASCADE')


def check_df_equal(a, b):
    eq_rows = (a == b).all(axis='columns')

    if eq_rows.all():
        return True

    else:
        print('Difference')
        print('A:')
        print(a.loc[-eq_rows])
        print('B:')
        print(b.loc[-eq_rows])

        return False


def assert_idx_equal(a, b):
    a = sorted(list(a))
    b = sorted(list(b))

    assert(a == b)

@pytest.mark.usefixtures('test_schema')
def test_cloudpickle():
    ds = DataStore(DBCONNSTR, schema='test')

    tbl = ds.get_table('test', TEST_SCHEMA)

    cloudpickle.dumps([ds, tbl])


@pytest.mark.usefixtures('test_schema')
def test_simple():
    ds = DataStore(DBCONNSTR, schema='test')

    tbl = ds.get_table('test', TEST_SCHEMA)

    tbl.store(TEST_DF)


@pytest.mark.usefixtures('test_schema')
def test_store_less_values():
    ds = DataStore(DBCONNSTR, schema='test')

    tbl = ds.get_table('test', TEST_SCHEMA)

    tbl.store(TEST_DF)
    assert_idx_equal(tbl.get_metadata().index, TEST_DF.index)

    tbl.store(TEST_DF[:5])
    assert_idx_equal(tbl.get_metadata().index, TEST_DF[:5].index)


@pytest.mark.usefixtures('test_schema')
def test_get_process_ids():
    ds = DataStore(DBCONNSTR, schema='test')

    tbl1 = ds.get_table('tbl1', TEST_SCHEMA)
    tbl2 = ds.get_table('tbl2', TEST_SCHEMA)

    tbl1.store(TEST_DF)

    idx = ds.get_process_ids([tbl1], tbl2)
    assert(list(idx) == list(TEST_DF.index))

    tbl2.store(tbl1.get_data())

    upd_df = TEST_DF[:5].copy()
    upd_df['a'] += 1

    tbl1.store_chunk(upd_df)

    idx = ds.get_process_ids([tbl1], tbl2)
    assert(list(idx) == list(upd_df.index))


@pytest.mark.usefixtures('test_schema')
def test_gen_process():
    ds = DataStore(DBCONNSTR, schema='test')

    tbl1_gen = ds.get_table('tbl1_gen', TEST_SCHEMA)
    tbl1 = ds.get_table('tbl1', TEST_SCHEMA)

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

    assert(check_df_equal(tbl1_gen.get_data(), TEST_DF))
    assert(check_df_equal(tbl1.get_data(), TEST_DF))

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

    assert(check_df_equal(tbl1_gen.get_data(), TEST_DF[:5]))
    assert(check_df_equal(tbl1.get_data(), TEST_DF[:5]))


@pytest.mark.usefixtures('test_schema')
def test_inc_process_modify_values() -> None:
    ds = DataStore(DBCONNSTR, schema='test')

    tbl1 = ds.get_table('tbl1', TEST_SCHEMA)
    tbl2 = ds.get_table('tbl2', TEST_SCHEMA)

    def id_func(df):
        return df

    tbl1.store(TEST_DF)

    inc_process(ds, [tbl1], tbl2, id_func)

    assert(check_df_equal(tbl2.get_data(), TEST_DF))

    ##########################
    tbl1.store(TEST_DF_INC1)

    inc_process(ds, [tbl1], tbl2, id_func)

    assert(check_df_equal(tbl2.get_data(), TEST_DF_INC1))


@pytest.mark.usefixtures('test_schema')
def test_inc_process_delete_values_from_input() -> None:
    ds = DataStore(DBCONNSTR, schema='test')

    tbl1 = ds.get_table('tbl1', TEST_SCHEMA)
    tbl2 = ds.get_table('tbl2', TEST_SCHEMA)

    def id_func(df):
        return df

    tbl1.store(TEST_DF)

    inc_process(ds, [tbl1], tbl2, id_func)

    assert(check_df_equal(tbl2.get_data(), TEST_DF))

    ##########################
    tbl1.store(TEST_DF[:5])

    inc_process(ds, [tbl1], tbl2, id_func, chunksize=2)

    assert(check_df_equal(tbl2.get_data(), TEST_DF[:5]))


@pytest.mark.usefixtures('test_schema')
def test_inc_process_delete_values_from_proc() -> None:
    ds = DataStore(DBCONNSTR, schema='test')

    tbl1 = ds.get_table('tbl1', TEST_SCHEMA)
    tbl2 = ds.get_table('tbl2', TEST_SCHEMA)

    def id_func(df):
        return df[:5]

    tbl2.store(TEST_DF)

    tbl1.store(TEST_DF)

    inc_process(ds, [tbl1], tbl2, id_func)

    assert(check_df_equal(tbl2.get_data(), TEST_DF[:5]))


@pytest.mark.usefixtures('test_schema')
def test_inc_process_proc_no_change() -> None:
    ds = DataStore(DBCONNSTR, schema='test')

    tbl1 = ds.get_table('tbl1', TEST_SCHEMA)
    tbl2 = ds.get_table('tbl2', TEST_SCHEMA)

    def id_func(df):
        return TEST_DF

    tbl2.store(TEST_DF)
    tbl1.store(TEST_DF)

    assert(len(ds.get_process_ids([tbl1], tbl2)) == len(TEST_DF))

    inc_process(ds, [tbl1], tbl2, id_func)

    assert(len(ds.get_process_ids([tbl1], tbl2)) == 0)

    tbl1.store(TEST_DF_INC1)

    assert(len(ds.get_process_ids([tbl1], tbl2)) == len(TEST_DF))

    inc_process(ds, [tbl1], tbl2, id_func)

    assert(len(ds.get_process_ids([tbl1], tbl2)) == 0)

# TODO тест inc_process 2->1
# TODO тест inc_process 2->1, удаление строки, 2->1


@pytest.mark.usefixtures('test_schema')
def test_gen_process_many():
    ds = DataStore(DBCONNSTR, schema='test')

    tbl_gen = ds.get_table('tbl_gen', TEST_SCHEMA)
    tbl1_gen = ds.get_table('tbl1_gen', TEST_SCHEMA)
    tbl2_gen = ds.get_table('tbl2_gen', TEST_SCHEMA)
    tbl3_gen = ds.get_table('tbl3_gen', TEST_SCHEMA)
    tbl = ds.get_table('tbl', TEST_SCHEMA)
    tbl1 = ds.get_table('tbl1', TEST_SCHEMA)
    tbl2 = ds.get_table('tbl2', TEST_SCHEMA)
    tbl3 = ds.get_table('tbl3', TEST_SCHEMA)

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

    assert(check_df_equal(tbl_gen.get_data(), TEST_DF))
    assert(check_df_equal(tbl1_gen.get_data(), TEST_DF_INC1))
    assert(check_df_equal(tbl2_gen.get_data(), TEST_DF_INC2))
    assert(check_df_equal(tbl3_gen.get_data(), TEST_DF_INC3))
    assert(check_df_equal(tbl.get_data(), TEST_DF))
    assert(check_df_equal(tbl1.get_data(), TEST_DF_INC1))
    assert(check_df_equal(tbl2.get_data(), TEST_DF_INC2))
    assert(check_df_equal(tbl3.get_data(), TEST_DF_INC3))


@pytest.mark.usefixtures('test_schema')
def test_inc_process_many_modify_values() -> None:
    ds = DataStore(DBCONNSTR, schema='test')

    tbl = ds.get_table('tbl', TEST_SCHEMA)
    tbl1 = ds.get_table('tbl1', TEST_SCHEMA)
    tbl2 = ds.get_table('tbl2', TEST_SCHEMA)
    tbl3 = ds.get_table('tbl3', TEST_SCHEMA)

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

    assert(check_df_equal(tbl1.get_data(), TEST_DF_INC1))
    assert(check_df_equal(tbl2.get_data(), TEST_DF_INC2))
    assert(check_df_equal(tbl3.get_data(), TEST_DF_INC3))

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

    assert(check_df_equal(tbl1.get_data(), TEST_DF_INC1[:5]))
    assert(check_df_equal(tbl2.get_data(), TEST_DF_INC2[:5]))
    assert(check_df_equal(tbl3.get_data(), TEST_DF_INC3[:5]))

    ##########################

    tbl.store_chunk(TEST_DF[5:])

    inc_process_many(ds, [tbl], [tbl1, tbl2, tbl3], inc_func)
    assert(check_df_equal(tbl1.get_data(), TEST_DF_INC1))
    assert(check_df_equal(tbl2.get_data(), TEST_DF_INC2))
    assert(check_df_equal(tbl3.get_data(), TEST_DF_INC3))


@pytest.mark.usefixtures('test_schema')
def test_inc_process_many_several_outputs():
    ds = DataStore(DBCONNSTR, schema='test')

    BAD_IDXS = ['id_0', 'id_1', 'id_5', 'id_8']
    GOOD_IDXS = ['id_2', 'id_3', 'id_4', 'id_6', 'id_7', 'id_9']

    tbl = ds.get_table('tbl', TEST_SCHEMA)
    tbl_good = ds.get_table('tbl_good', TEST_SCHEMA)
    tbl_bad = ds.get_table('tbl_bad', TEST_SCHEMA)

    tbl.store(TEST_DF)

    def inc_func(df):
        df_good = df.drop(index=BAD_IDXS)
        df_bad = df.drop(index=GOOD_IDXS)
        return df_good, df_bad

    inc_process_many(ds, [tbl], [tbl_good, tbl_bad], inc_func)
    assert(check_df_equal(tbl.get_data(), TEST_DF))
    assert(check_df_equal(tbl_good.get_data(), TEST_DF.loc[GOOD_IDXS]))
    assert(check_df_equal(tbl_bad.get_data(), TEST_DF.loc[BAD_IDXS]))

    # Check this not delete the tables
    inc_process_many(ds, [tbl], [tbl_good, tbl_bad], inc_func)
    assert(check_df_equal(tbl.get_data(), TEST_DF))
    assert(check_df_equal(tbl_good.get_data(), TEST_DF.loc[GOOD_IDXS]))
    assert(check_df_equal(tbl_bad.get_data(), TEST_DF.loc[BAD_IDXS]))


@pytest.mark.usefixtures('test_schema')
def test_data_catalog() -> None:
    data_catalog = DataCatalog(
        catalog={
            'test_df': AbstractDataTable([Column('a', Numeric)])
        },
        connstr=DBCONNSTR,
        schema='test_schema'
    )

    tbl = data_catalog.get_data_table('test_df')
    tbl.store(TEST_DF)
    assert(check_df_equal(tbl.get_data(), TEST_DF))


@pytest.mark.usefixtures('test_schema')
def test_store_node() -> None:
    data_catalog = DataCatalog(
        catalog={
            'test_df': AbstractDataTable([Column('a', Numeric)])
        },
        connstr=DBCONNSTR,
        schema='test_schema'
    )

    def proc_func():
        return TEST_DF

    store_node = StoreNode(
        proc_func=proc_func,
        outputs=['test_df']
    )
    store_node.run(data_catalog=data_catalog)

    tbl = data_catalog.get_data_table('test_df')
    assert(check_df_equal(tbl.get_data(), TEST_DF))


@pytest.mark.usefixtures('test_schema')
def test_python_node() -> None:
    data_catalog = DataCatalog(
        catalog={
            'test_df': AbstractDataTable([Column('a', Numeric)]),
            'test_df_inc1': AbstractDataTable([Column('a', Numeric)]),
            'test_df_inc2': AbstractDataTable([Column('a', Numeric)]),
            'test_df_inc3': AbstractDataTable([Column('a', Numeric)])
        },
        connstr=DBCONNSTR,
        schema='test_schema'
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

    assert(check_df_equal(tbl.get_data(), TEST_DF))
    assert(check_df_equal(tbl1.get_data(), TEST_DF_INC1))
    assert(check_df_equal(tbl2.get_data(), TEST_DF_INC2))
    assert(check_df_equal(tbl3.get_data(), TEST_DF_INC3))


@pytest.fixture
def temp_dir():
    old_cwd = Path.cwd()
    with tempfile.TemporaryDirectory() as tmp_dir_path:
        os.chdir(tmp_dir_path)
        yield 'ok'
        os.chdir(old_cwd)


@pytest.mark.usefixtures('test_schema', 'temp_dir')
def test_label_studio_node() -> None:
    data_catalog = DataCatalog(
        catalog={
            'data_input': AbstractDataTable([Column('data', JSON)]),
            'data_annotation': AbstractDataTable([Column('data', JSON)])
        },
        connstr=DBCONNSTR,
        schema='test_schema'
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


@pytest.mark.usefixtures('test_schema')
def test_pipeline() -> None:
    data_catalog = DataCatalog(
        catalog={
            'test_df': AbstractDataTable([Column('a', Numeric)]),
            'test_df_inc1': AbstractDataTable([Column('a', Numeric)]),
            'test_df_inc2': AbstractDataTable([Column('a', Numeric)]),
            'test_df_inc3': AbstractDataTable([Column('a', Numeric)])
        },
        connstr=DBCONNSTR,
        schema='test_schema'
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

    assert(check_df_equal(tbl.get_data(), TEST_DF))
    assert(check_df_equal(tbl1.get_data(), TEST_DF_INC1))
    assert(check_df_equal(tbl2.get_data(), TEST_DF_INC2))
    assert(check_df_equal(tbl3.get_data(), TEST_DF_INC3))
