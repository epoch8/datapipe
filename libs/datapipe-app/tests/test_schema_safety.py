from __future__ import annotations

import os
import uuid

import pytest
from sqlalchemy import Column, Integer, String, inspect, text

from datapipe.store.database import DBConn, ensure_db_schema
from datapipe_app.observability.db import ObservabilityStore
from datapipe_app.observability.schema_resolution import assert_safe_drop_schema, is_datapipe_owned_table

pytestmark_postgres = pytest.mark.skipif(
    os.environ.get("TEST_DB_ENV") == "sqlite",
    reason="Postgres schema isolation tests require TEST_DB_ENV=postgres",
)


def _postgres_connstr() -> str:
    pg_host = os.getenv("POSTGRES_HOST", "localhost")
    pg_port = os.getenv("POSTGRES_PORT", "5432")
    return f"postgresql://postgres:password@{pg_host}:{pg_port}/postgres"


def _fresh_schema_name() -> str:
    return f"test_obs_schema_{uuid.uuid4().hex[:12]}"


@pytestmark_postgres
def test_observability_tables_do_not_touch_public_ls_tables() -> None:
    schema = _fresh_schema_name()
    connstr = _postgres_connstr()
    dbconn = DBConn(connstr, schema)
    ensure_db_schema(dbconn)

    with dbconn.con.begin() as con:
        con.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS public.fake_ls_task (
                    id INTEGER PRIMARY KEY,
                    data TEXT
                )
                """
            )
        )
        con.execute(text("INSERT INTO public.fake_ls_task (id, data) VALUES (1, 'keep') ON CONFLICT DO NOTHING"))

    store = ObservabilityStore.from_url(connstr, schema=schema)
    store.register_pipeline("demo", display_name="Demo")
    store.create_run("demo", trigger="api:pipeline")

    with dbconn.con.begin() as con:
        ls_count = con.execute(text("SELECT COUNT(*) FROM public.fake_ls_task")).scalar()
        obs_count = con.execute(
            text(f'SELECT COUNT(*) FROM "{schema}".pipeline_runs')
        ).scalar()

    assert ls_count == 1
    assert obs_count == 1

    with dbconn.con.begin() as con:
        con.execute(text("DROP TABLE IF EXISTS public.fake_ls_task"))
        con.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))


def test_is_datapipe_owned_table() -> None:
    assert is_datapipe_owned_table("pipeline_runs")
    assert not is_datapipe_owned_table("fake_ls_task")


def test_assert_safe_drop_schema_blocks_public() -> None:
    with pytest.raises(RuntimeError):
        assert_safe_drop_schema("public", allow_cascade=False)
