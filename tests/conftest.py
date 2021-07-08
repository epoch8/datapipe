import pytest

import os
from sqlalchemy import create_engine
import tempfile
from pathlib import Path


from datapipe.store.database import DBConn


@pytest.fixture
def tmp_dir():
    with tempfile.TemporaryDirectory() as d:
        d = Path(d)
        yield d


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
