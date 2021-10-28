import tempfile
import distutils.util

from pathlib import Path
import pytest
import os

import pandas as pd
from sqlalchemy import create_engine

from datapipe.store.database import DBConn


@pytest.fixture
def tmp_dir():
    with tempfile.TemporaryDirectory() as d:
        d = Path(d)
        yield d


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
        pg_host = os.getenv("POSTGRES_HOST", "localhost")
        pg_port = os.getenv("POSTGRES_PORT", "5432")
        DBCONNSTR = f'postgresql://postgres:password@{pg_host}:{pg_port}/postgres'
        DB_TEST_SCHEMA = 'test'
    else:
        DBCONNSTR = 'sqlite:///:memory:'
        DB_TEST_SCHEMA = None

    if DB_TEST_SCHEMA:
        eng = create_engine(DBCONNSTR)

        try:
            eng.execute(f'DROP SCHEMA {DB_TEST_SCHEMA} CASCADE')
        except Exception:
            pass

        eng.execute(f'CREATE SCHEMA {DB_TEST_SCHEMA}')

        yield DBConn(DBCONNSTR, DB_TEST_SCHEMA)

        eng.execute(f'DROP SCHEMA {DB_TEST_SCHEMA} CASCADE')

    else:
        yield DBConn(DBCONNSTR, DB_TEST_SCHEMA)


# Label Studio
TEST_LABEL_STUDIO = bool(distutils.util.strtobool(os.environ.get('TEST_LABEL_STUDIO', 'False')))


@pytest.fixture
def ls_url_and_auth(tmp_dir):
    ls_host = os.environ.get('LABEL_STUDIO_HOST', 'localhost')
    ls_port = os.environ.get('LABEL_STUDIO_PORT', '8080')
    ls_url = f"http://{ls_host}:{ls_port}/"
    auth = ('test@epoch8.co', 'qwerty123')
    yield ls_url, auth
