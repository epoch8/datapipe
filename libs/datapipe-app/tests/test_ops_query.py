import re
import tempfile
from unittest.mock import patch

import pytest
from datapipe.compute import Catalog, Table
from datapipe.datatable import DataStore
from datapipe.store.database import DBConn, TableStoreDB
from sqlalchemy import Column, Integer, String

from datapipe_app.ops.ops_query import OpsQuery, format_snapshot_label
from datapipe_app.ops.specs import OpsColumn


def test_snapshot_labels_do_not_generate_semver():
    label = format_snapshot_label("20260707_1237_27beda99", mode="short_id")

    assert not re.search(r"v\d+\.\d+", label)
    assert label.startswith("2026070")


def test_query_returns_empty_for_catalog_table_not_created_yet():
    with tempfile.TemporaryDirectory() as tmpdir:
        dbconn = DBConn(f"sqlite+pysqlite3:///{tmpdir}/store.sqlite")
        ds = DataStore(dbconn, create_meta_table=True)
        catalog = Catalog({
            "future_table": Table(
                TableStoreDB(
                    name="future_table",
                    dbconn=dbconn,
                    data_sql_schema=[
                        Column("id", Integer(), primary_key=True),
                        Column("name", String()),
                    ],
                    create_table=False,
                )
            )
        })

        rows, total = OpsQuery(ds, catalog).rows(
            "future_table",
            allowed_columns=[OpsColumn("id", "ID", "id"), OpsColumn("name", "Name", "name")],
        )

    assert rows == []
    assert total == 0


def test_physical_table_exists_checks_table_schema(ops_app):
    calls = []

    class FakeInspector:
        def has_table(self, name, schema=None):
            calls.append((name, schema))
            return True

    table = ops_app.catalog.catalog["input"].store.data_table
    table.schema = "custom_schema"

    with patch("datapipe_app.ops.ops_query.inspect", return_value=FakeInspector()):
        assert OpsQuery(ops_app.ds, ops_app.catalog)._physical_table_exists(table)

    assert calls == [("input", "custom_schema")]
