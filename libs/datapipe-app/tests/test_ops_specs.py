import re
import tempfile
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from datapipe.compute import Catalog, Table
from datapipe.datatable import DataStore
from datapipe.store.database import DBConn, TableStoreDB
from sqlalchemy import Column, Integer, String

from datapipe_app.ops_query import OpsQuery
from datapipe_app.spec_registry import OpsSpecRegistry, OpsSpecValidationError
from datapipe_app.specs import (
    DatapipeOpsSpec,
    OpsColumn,
    OpsColumnGroup,
    OpsDataSpec,
    OpsMetricTableSpec,
)
from datapipe_app.ops_query import format_snapshot_label


def input_spec(**overrides):
    values = {
        "id": "catalog_tags",
        "title": "Catalog Tags",
        "description": "Tag quality checks over catalog events.",
        "data": OpsDataSpec(tables=["input"]),
        "metrics": [
            OpsMetricTableSpec(
                id="input_metrics",
                title="Input metrics",
                table="input",
                metric_source="input",
                primary_key_columns=["id"],
                entity_links={"item": "id"},
                primary_columns=[OpsColumn("id", "ID", "id", filterable=True)],
                metric_columns=[OpsColumn("value", "Value", "v")],
                filters=[OpsColumn("value_filter", "Value", "v", filterable=True)],
                best_metric_column="v",
                default_sort=[("id", "asc")],
            )
        ],
    }
    values.update(overrides)
    return DatapipeOpsSpec(**values)


def test_duplicate_spec_id_validation():
    registry = OpsSpecRegistry()
    registry.add_many([input_spec()])
    with pytest.raises(OpsSpecValidationError, match="already registered"):
        registry.add_many([input_spec()])


def test_missing_table_validation(ops_app):
    spec = input_spec(data=OpsDataSpec(tables=["missing_table"]))
    with pytest.raises(OpsSpecValidationError, match='missing table "missing_table"'):
        ops_app.add_specs([spec])


def test_missing_column_validation(ops_app):
    broken = input_spec(
        metrics=[
            OpsMetricTableSpec(
                id="broken",
                title="Broken",
                table="input",
                metric_source="input",
                primary_key_columns=["id"],
                entity_links={},
                primary_columns=[OpsColumn("missing", "Missing", "missing_column")],
                metric_columns=[],
            )
        ]
    )
    with pytest.raises(OpsSpecValidationError, match='missing column "missing_column"'):
        ops_app.add_specs([broken])


def test_ops_specs_metadata_endpoint(ops_app):
    ops_app.add_specs([input_spec()])
    client = TestClient(ops_app)

    res = client.get("/api/v1alpha3/pipelines/test_pipeline/ops-specs")

    assert res.status_code == 200
    payload = res.json()
    assert payload["pipeline_id"] == "test_pipeline"
    assert payload["specs"][0]["title"] == "Catalog Tags"
    assert payload["specs"][0]["metric_tables_count"] == 1


def test_grouped_metric_columns_serialize(ops_app):
    grouped = input_spec(
        metrics=[
            OpsMetricTableSpec(
                id="grouped",
                title="Grouped",
                table="input",
                metric_source="input",
                primary_key_columns=["id"],
                entity_links={},
                primary_columns=[OpsColumn("id", "ID", "id")],
                metric_columns=[
                    OpsColumnGroup(
                        "Quality",
                        [OpsColumn("precision", "Precision", "v")],
                    )
                ],
            )
        ]
    )
    ops_app.add_specs([grouped])
    client = TestClient(ops_app)

    res = client.get("/api/v1alpha3/pipelines/test_pipeline/ops-specs/catalog_tags")

    assert res.status_code == 200
    table = res.json()["metrics"][0]
    assert table["metric_columns"][0]["label"] == "Quality"
    assert table["metric_columns"][0]["columns"][0]["label"] == "Precision"


def test_metric_rows_reject_unknown_sort(ops_app):
    ops_app.add_specs([input_spec()])
    client = TestClient(ops_app)

    res = client.get(
        "/api/v1alpha3/pipelines/test_pipeline/ops-specs/catalog_tags/metrics/input_metrics/rows",
        params={"sort_by": "not_configured"},
    )

    assert res.status_code == 400
    assert "not_configured" in res.text


def test_metric_rows_return_configured_columns(ops_app):
    ops_app.add_specs([input_spec()])
    client = TestClient(ops_app)

    res = client.get(
        "/api/v1alpha3/pipelines/test_pipeline/ops-specs/catalog_tags/metrics/input_metrics/rows",
        params={"sort_by": "id", "sort_dir": "asc"},
    )

    assert res.status_code == 200
    payload = res.json()
    assert payload["total"] == 2
    assert payload["rows"][0] == {"id": 1, "v": "a", "item_id": 1}


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

    with patch("datapipe_app.ops_query.inspect", return_value=FakeInspector()):
        assert OpsQuery(ops_app.ds, ops_app.catalog)._physical_table_exists(table)

    assert calls == [("input", "custom_schema")]
