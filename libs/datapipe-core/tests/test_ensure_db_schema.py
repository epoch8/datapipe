from __future__ import annotations

import os
import uuid

import pytest
from sqlalchemy import Column, Integer, String, inspect, text

from datapipe.meta.sql_meta import SQLTableMeta
from datapipe.store.database import DBConn, TableStoreDB, ensure_db_schema

pytestmark_postgres = pytest.mark.skipif(
    os.environ.get("TEST_DB_ENV") == "sqlite",
    reason="Postgres schema tests require TEST_DB_ENV=postgres",
)


def _postgres_connstr() -> str:
    pg_host = os.getenv("POSTGRES_HOST", "localhost")
    pg_port = os.getenv("POSTGRES_PORT", "5432")
    return f"postgresql://postgres:password@{pg_host}:{pg_port}/postgres"


def _fresh_schema_name() -> str:
    return f"test_ensure_schema_{uuid.uuid4().hex[:12]}"


@pytest.fixture
def postgres_dbconn_fresh_schema():
    schema = _fresh_schema_name()
    dbconn = DBConn(_postgres_connstr(), schema)
    yield dbconn
    with dbconn.con.begin() as con:
        con.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))


def test_ensure_db_schema_noop_without_schema() -> None:
    dbconn = DBConn("sqlite:///:memory:", schema=None)
    ensure_db_schema(dbconn)


@pytestmark_postgres
def test_ensure_db_schema_creates_schema(postgres_dbconn_fresh_schema: DBConn) -> None:
    dbconn = postgres_dbconn_fresh_schema
    assert dbconn.schema not in inspect(dbconn.con).get_schema_names()

    ensure_db_schema(dbconn)

    assert dbconn.schema in inspect(dbconn.con).get_schema_names()


@pytestmark_postgres
def test_ensure_db_schema_is_idempotent(postgres_dbconn_fresh_schema: DBConn) -> None:
    dbconn = postgres_dbconn_fresh_schema

    ensure_db_schema(dbconn)
    ensure_db_schema(dbconn)

    assert dbconn.schema in inspect(dbconn.con).get_schema_names()


@pytestmark_postgres
def test_table_store_db_create_table_ensures_schema(postgres_dbconn_fresh_schema: DBConn) -> None:
    dbconn = postgres_dbconn_fresh_schema
    assert dbconn.schema not in inspect(dbconn.con).get_schema_names()

    TableStoreDB(
        dbconn=dbconn,
        name="items",
        data_sql_schema=[
            Column("id", Integer, primary_key=True),
            Column("name", String),
        ],
        create_table=True,
    )

    assert dbconn.schema in inspect(dbconn.con).get_schema_names()
    assert "items" in inspect(dbconn.con).get_table_names(schema=dbconn.schema)


@pytestmark_postgres
def test_sql_table_meta_create_table_ensures_schema(postgres_dbconn_fresh_schema: DBConn) -> None:
    dbconn = postgres_dbconn_fresh_schema
    assert dbconn.schema not in inspect(dbconn.con).get_schema_names()

    SQLTableMeta(
        dbconn=dbconn,
        name="items",
        primary_schema=[Column("id", Integer, primary_key=True)],
        create_table=True,
    )

    assert dbconn.schema in inspect(dbconn.con).get_schema_names()
    assert "items_meta" in inspect(dbconn.con).get_table_names(schema=dbconn.schema)


@pytestmark_postgres
def test_db_create_all_ensures_schema_before_metadata_create_all(postgres_dbconn_fresh_schema: DBConn) -> None:
    dbconn = postgres_dbconn_fresh_schema
    assert dbconn.schema not in inspect(dbconn.con).get_schema_names()

    TableStoreDB(
        dbconn=dbconn,
        name="catalog_table",
        data_sql_schema=[Column("id", Integer, primary_key=True)],
        create_table=False,
    )
    SQLTableMeta(
        dbconn=dbconn,
        name="catalog_table",
        primary_schema=[Column("id", Integer, primary_key=True)],
        create_table=False,
    )

    ensure_db_schema(dbconn)
    dbconn.sqla_metadata.create_all(dbconn.con)

    assert dbconn.schema in inspect(dbconn.con).get_schema_names()
    assert "catalog_table" in inspect(dbconn.con).get_table_names(schema=dbconn.schema)
    assert "catalog_table_meta" in inspect(dbconn.con).get_table_names(schema=dbconn.schema)


def test_db_create_all_force_recreate_drops_existing_data() -> None:
    dbconn = DBConn("sqlite:///:memory:", schema=None)
    store = TableStoreDB(
        dbconn=dbconn,
        name="items",
        data_sql_schema=[
            Column("id", Integer, primary_key=True),
            Column("name", String),
        ],
        create_table=True,
    )

    with dbconn.con.begin() as con:
        con.execute(store.data_table.insert().values(id=1, name="keep-me"))

    with dbconn.con.begin() as con:
        assert con.execute(text("SELECT COUNT(*) FROM items")).scalar() == 1

    dbconn.sqla_metadata.drop_all(dbconn.con)
    dbconn.sqla_metadata.create_all(dbconn.con)

    assert "items" in inspect(dbconn.con).get_table_names()
    with dbconn.con.begin() as con:
        assert con.execute(text("SELECT COUNT(*) FROM items")).scalar() == 0


def test_db_create_all_force_recreate_applies_new_columns() -> None:
    dbconn = DBConn("sqlite:///:memory:", schema=None)
    TableStoreDB(
        dbconn=dbconn,
        name="items",
        data_sql_schema=[
            Column("id", Integer, primary_key=True),
            Column("name", String),
        ],
        create_table=True,
    )

    assert {c["name"] for c in inspect(dbconn.con).get_columns("items")} == {"id", "name"}

    # Simulate a pipeline schema change that only exists in metadata after recreate.
    from sqlalchemy import MetaData, Table

    new_metadata = MetaData()
    Table(
        "items",
        new_metadata,
        Column("id", Integer, primary_key=True),
        Column("name", String),
        Column("extra", String),
    )
    dbconn.sqla_metadata = new_metadata

    dbconn.sqla_metadata.drop_all(dbconn.con)
    dbconn.sqla_metadata.create_all(dbconn.con)

    assert {c["name"] for c in inspect(dbconn.con).get_columns("items")} == {"id", "name", "extra"}
