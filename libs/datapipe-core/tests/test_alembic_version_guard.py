from __future__ import annotations

import pytest
from sqlalchemy import Column, Integer, String, text

from datapipe.store.database import DBConn, TableStoreDB
from datapipe.store.schema_sync import (
    AlembicManagedDatabaseError,
    alembic_version_is_present,
    refuse_if_alembic_managed,
)


def _stamp_alembic_version(dbconn: DBConn, version: str = "abc123def") -> None:
    with dbconn.con.begin() as con:
        con.execute(
            text(
                "CREATE TABLE alembic_version ("
                "version_num VARCHAR(32) NOT NULL, "
                "CONSTRAINT alembic_version_pkc PRIMARY KEY (version_num)"
                ")"
            )
        )
        con.execute(text("INSERT INTO alembic_version (version_num) VALUES (:v)"), {"v": version})


def test_alembic_version_absent_on_empty_db() -> None:
    dbconn = DBConn("sqlite:///:memory:", schema=None)
    assert alembic_version_is_present(dbconn.con) is False
    refuse_if_alembic_managed(dbconn.con)  # does not raise


def test_alembic_version_empty_table_is_not_stamped() -> None:
    dbconn = DBConn("sqlite:///:memory:", schema=None)
    with dbconn.con.begin() as con:
        con.execute(
            text(
                "CREATE TABLE alembic_version ("
                "version_num VARCHAR(32) NOT NULL, "
                "CONSTRAINT alembic_version_pkc PRIMARY KEY (version_num)"
                ")"
            )
        )
    assert alembic_version_is_present(dbconn.con) is False
    refuse_if_alembic_managed(dbconn.con)


def test_alembic_version_present_when_stamped() -> None:
    dbconn = DBConn("sqlite:///:memory:", schema=None)
    _stamp_alembic_version(dbconn)
    assert alembic_version_is_present(dbconn.con) is True
    with pytest.raises(AlembicManagedDatabaseError, match="Alembic revision"):
        refuse_if_alembic_managed(dbconn.con)


def test_refuse_blocks_before_mutating_existing_tables() -> None:
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

    _stamp_alembic_version(dbconn)

    with pytest.raises(AlembicManagedDatabaseError):
        refuse_if_alembic_managed(dbconn.con)

    with dbconn.con.begin() as con:
        assert con.execute(text("SELECT COUNT(*) FROM items")).scalar() == 1
        assert con.execute(text("SELECT version_num FROM alembic_version")).scalar() == "abc123def"
