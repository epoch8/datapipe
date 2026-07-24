from __future__ import annotations

import pytest
from sqlalchemy import Column, Integer, MetaData, String, Table, inspect, text

pytest.importorskip("alembic")

from datapipe.store.database import DBConn, TableStoreDB
from datapipe.store.schema_sync import _schemas_compatible, sync_sqla_metadata


def test_schemas_compatible_custom_schema_like_e2e_template() -> None:
    custom = "datapipe_e2e_detection"
    assert _schemas_compatible(custom, custom, default=custom)
    assert _schemas_compatible(None, custom, default=custom)
    assert _schemas_compatible(custom, None, default=custom)
    # Do not treat public tables as the custom schema.
    assert not _schemas_compatible("public", custom, default=custom)


def test_schemas_compatible_default_public() -> None:
    assert _schemas_compatible(None, None, default=None)
    assert _schemas_compatible(None, "public", default=None)
    assert _schemas_compatible("public", None, default=None)


def test_sync_adds_missing_column_without_dropping_rows() -> None:
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

    # New metadata with an extra column (simulates pipeline schema change).
    new_metadata = MetaData()
    Table(
        "items",
        new_metadata,
        Column("id", Integer, primary_key=True),
        Column("name", String),
        Column("extra", String),
    )

    applied = sync_sqla_metadata(dbconn.con, new_metadata)
    assert applied >= 1

    cols = {c["name"] for c in inspect(dbconn.con).get_columns("items")}
    assert cols == {"id", "name", "extra"}

    with dbconn.con.begin() as con:
        row = con.execute(text("SELECT id, name FROM items")).one()
    assert tuple(row) == (1, "keep-me")


def test_sync_is_noop_when_already_in_sync() -> None:
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

    applied = sync_sqla_metadata(dbconn.con, dbconn.sqla_metadata)
    assert applied == 0


def test_sync_after_create_all_does_not_recreate_tables() -> None:
    """Regression: Alembic must not CREATE TABLE again after create_all."""
    dbconn = DBConn("sqlite:///:memory:", schema=None)
    TableStoreDB(
        dbconn=dbconn,
        name="items",
        data_sql_schema=[
            Column("id", Integer, primary_key=True),
            Column("name", String),
        ],
        create_table=False,
    )
    dbconn.sqla_metadata.create_all(dbconn.con)

    # Must not raise DuplicateTable / operational error.
    applied = sync_sqla_metadata(dbconn.con, dbconn.sqla_metadata)
    assert applied == 0
    assert "items" in inspect(dbconn.con).get_table_names()


def test_sync_does_not_drop_unrelated_tables() -> None:
    dbconn = DBConn("sqlite:///:memory:", schema=None)
    with dbconn.con.begin() as con:
        con.execute(text("CREATE TABLE orphan (id INTEGER PRIMARY KEY)"))

    metadata = MetaData()
    Table("items", metadata, Column("id", Integer, primary_key=True))
    metadata.create_all(dbconn.con)

    sync_sqla_metadata(dbconn.con, metadata)

    names = set(inspect(dbconn.con).get_table_names())
    assert "orphan" in names
    assert "items" in names
