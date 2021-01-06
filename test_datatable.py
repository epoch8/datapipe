import pytest

import pandas as pd
from sqlalchemy import create_engine, Column, Numeric

from c12n_pipe.datatable import DataStore, DataTable

DBCONNSTR = 'postgresql://postgres:password@localhost:5432/postgres'

TEST_SCHEMA = [
    Column('a', Numeric),
]

TEST_DF = pd.DataFrame(
    {
        'a': range(10)
    },
    index=[f'id_{i}' for i in range(10)]
)

@pytest.fixture
def test_schema():
    eng = create_engine(DBCONNSTR)

    eng.execute('CREATE SCHEMA test')

    yield 'ok'

    eng.execute('DROP SCHEMA test CASCADE')

@pytest.mark.usefixtures('test_schema')
def test_simple():
    ds = DataStore(DBCONNSTR, schema='test')

    tbl = ds.get_table('test', TEST_SCHEMA)

    tbl.store()


@pytest.mark.usefixtures('test_schema')
def test_inc_process_1_to_1():
    ds = DataStore(DBCONNSTR, schema='test')

    tbl1 = ds.get_table('tbl1', TEST_SCHEMA)
    tbl2 = ds.get_table('tbl2', TEST_SCHEMA)

    tbl1.store(TEST_DF)

    idx = ds.get_process_ids([tbl1], tbl2)
    assert(list(idx) == list(TEST_DF.index))
    
    tbl2.store(tbl1.get_data())

    upd_df = TEST_DF[:5].copy()
    upd_df['a'] += 1

    tbl1.store(upd_df)

    idx = ds.get_process_ids([tbl1], tbl2)
    assert(list(idx) == list(upd_df.index))


# TODO тест gen_process
# TODO тест inc_process 2->1
# TODO тест inc_process 2->1, удаление строки, 2->1
