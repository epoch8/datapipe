import os

os.environ["SQLALCHEMY_WARN_20"] = "1"

import random
import tempfile
from pathlib import Path

import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from datapipe.store.database import DBConn


@pytest.fixture
def tmp_dir():
    with tempfile.TemporaryDirectory() as d:
        d = Path(d)
        yield d


def assert_idx_equal(a, b):
    a = sorted(list(a))
    b = sorted(list(b))

    assert a == b


def assert_df_equal(a: pd.DataFrame, b: pd.DataFrame) -> bool:
    assert_idx_equal(a.index, b.index)

    eq_rows = (a == b).all(axis="columns")

    if eq_rows.all():
        return True

    else:
        print("Difference")
        print("A:")
        print(a.loc[-eq_rows])
        print("B:")
        print(b.loc[-eq_rows])

        raise AssertionError


@pytest.fixture
def dbconn():
    if os.environ.get("TEST_DB_ENV") == "sqlite":
        DBCONNSTR = "sqlite+pysqlite3:///:memory:"
        DB_TEST_SCHEMA = None
    else:
        pg_host = os.getenv("POSTGRES_HOST", "localhost")
        pg_port = os.getenv("POSTGRES_PORT", "5432")
        DBCONNSTR = f"postgresql://postgres:password@{pg_host}:{pg_port}/postgres"
        DB_TEST_SCHEMA = "test"

    if DB_TEST_SCHEMA:
        eng = create_engine(DBCONNSTR)

        try:
            with eng.begin() as conn:
                conn.execute(text(f"DROP SCHEMA {DB_TEST_SCHEMA} CASCADE"))
        except Exception:
            pass

        with eng.begin() as conn:
            conn.execute(text(f"CREATE SCHEMA {DB_TEST_SCHEMA}"))

        yield DBConn(DBCONNSTR, DB_TEST_SCHEMA)

        with eng.begin() as conn:
            conn.execute(text(f"DROP SCHEMA {DB_TEST_SCHEMA} CASCADE"))

    else:
        yield DBConn(DBCONNSTR, DB_TEST_SCHEMA)


@pytest.fixture
def redis_conn():
    import redis

    redis_host = os.getenv("REDIS_HOST", "localhost")
    redis_port = os.getenv("REDIS_PORT", "6379")
    conn = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)  # type: ignore
    if keys := conn.keys():
        conn.delete(*keys)
    yield f"redis://{redis_host}:{redis_port}"


@pytest.fixture(scope="function")
def elastic_conn():
    from elasticsearch import Elasticsearch

    elastic_host = os.getenv("ELASTIC_HOST", "localhost")
    elastic_port = os.getenv("ELASTIC_PORT", "9200")
    es_kwargs = {"hosts": [f"http://{elastic_host}:{elastic_port}"]}

    # elastic is bad at concurrent modification access to the same index
    test_index = f"test_index_{random.randint(0, 2347682)}"

    conn = Elasticsearch(**es_kwargs)  # type: ignore
    if not conn.indices.exists(index=test_index):
        conn.indices.create(index=test_index)

    if int(conn.cat.count(index=test_index, format="json")[0]["count"]) > 0:  # type: ignore
        conn.delete_by_query(index=test_index, query={"match_all": {}})

    yield {"es_kwargs": es_kwargs, "index": test_index}

    conn.indices.delete(index=test_index)
