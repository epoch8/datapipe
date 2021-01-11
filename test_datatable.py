import pytest
import os

import cloudpickle
import pandas as pd
from sqlalchemy import create_engine, Column, Numeric

from c12n_pipe.datatable import DataStore, gen_process, inc_process

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

    tbl1 = ds.get_table('tbl1', TEST_SCHEMA)

    def gen():
        yield TEST_DF

    gen_process(
        tbl1,
        gen
    )

    assert(check_df_equal(tbl1.get_data(), TEST_DF))

    def gen2():
        yield TEST_DF[:5]
    
    gen_process(
        tbl1,
        gen2
    )

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


### FIXME make it work!!!

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
