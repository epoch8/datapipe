import pytest
import os

import pandas as pd
from sqlalchemy import create_engine

from datapipe.store.table_store_sql import DBConn


def assert_idx_equal(a, b):
    a = sorted(list(a))
    b = sorted(list(b))

    assert(a == b)


def assert_df_equal(a: pd.DataFrame, b: pd.DataFrame) -> bool:
    assert_idx_equal(a.index, b.index)

    eq_rows = (a == b).all(axis='columns')

    if eq_rows.all():
        return True

    else:
        print('Difference')
        print('A:')
        print(a.loc[-eq_rows])
        print('B:')
        print(b.loc[-eq_rows])

        raise AssertionError


@pytest.fixture
def dbconn():
    if os.environ.get('TEST_DB_ENV') == 'postgres':
        DBCONNSTR = f'postgresql://postgres:password@{os.getenv("POSTGRES_HOST", "localhost")}:{os.getenv("POSTGRES_PORT", 5432)}/postgres'
        DB_TEST_SCHEMA = 'test'
    else:
        DBCONNSTR = 'sqlite:///:memory:'
        DB_TEST_SCHEMA = None

    if DB_TEST_SCHEMA:
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

