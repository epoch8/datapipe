import os
import tempfile
import time
from pathlib import Path

import pytest
import requests
from datapipe.store.database import DBConn
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

from datapipe_label_studio.sdk_utils import login_and_get_token, sign_up

if (Path(__file__).parent / ".env").exists():
    load_dotenv(Path(__file__).parent / ".env")


@pytest.fixture
def tmp_dir():
    with tempfile.TemporaryDirectory() as d:
        d = Path(d)
        yield d


@pytest.fixture
def dbconn():
    if os.environ.get("TEST_DB_ENV") == "sqlite":
        DBCONNSTR = "sqlite:///:memory:"
        DB_TEST_SCHEMA = None
    else:
        pg_host = os.getenv("POSTGRES_HOST", "localhost")
        pg_port = os.getenv("POSTGRES_PORT", "5432")
        DBCONNSTR = f"postgresql+psycopg://postgres:password@{pg_host}:{pg_port}/postgres"
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


@pytest.fixture(scope="session")
def ls_url() -> str:
    ls_host = os.environ.get("LABEL_STUDIO_HOST", "localhost")
    ls_port = os.environ.get("LABEL_STUDIO_PORT", "8080")
    return f"http://{ls_host}:{ls_port}"


def _get_ls_api_key_with_retries(
    ls_url: str,
    email: str,
    password: str,
    attempts: int = 60,
    sleep_seconds: float = 1.0,
) -> str:
    last_error = None
    for _ in range(attempts):
        try:
            api_key = sign_up(ls_url, email, password)
            if api_key is None:
                api_key = login_and_get_token(ls_url, email, password)
            return api_key
        except (requests.exceptions.RequestException, ValueError, KeyError) as exc:
            last_error = exc
            time.sleep(sleep_seconds)

    if last_error is not None:
        raise RuntimeError(
            f"Could not obtain Label Studio API token from {ls_url} after {attempts} attempts"
        ) from last_error
    raise RuntimeError(f"Could not obtain Label Studio API token from {ls_url}")


@pytest.fixture(scope="session")
def ls_url_and_api_key(ls_url):
    api_key = _get_ls_api_key_with_retries(ls_url, "test@epoch8.com", "qwerty123")
    yield ls_url, api_key
