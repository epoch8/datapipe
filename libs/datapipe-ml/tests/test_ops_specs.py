import tempfile

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from datapipe.compute import Catalog, Table
from datapipe.datatable import DataStore
from datapipe.store.database import DBConn, TableStoreDB
from sqlalchemy import Column, Integer, String

from datapipe_app.errors import OpsSpecValidationError
from datapipe_app.specs import (
    OpsColumn,
    OpsColumnGroup,
    OpsFilterRule,
    OpsMetricTableSpec,
)
from datapipe_ml.ops_spec_metrics import latest_eval_metric_from_specs
from datapipe_ml.ops_specs import DatapipeOpsSpec, OpsDataSpec
from datapipe_ml.spec_registry import OpsSpecRegistry


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


def test_latest_eval_metric_from_specs():
    with tempfile.TemporaryDirectory() as tmpdir:
        dbconn = DBConn(f"sqlite+pysqlite3:///{tmpdir}/store.sqlite")
        ds = DataStore(dbconn, create_meta_table=True)
        catalog = Catalog({
            "eval": Table(
                TableStoreDB(
                    name="eval",
                    dbconn=dbconn,
                    data_sql_schema=[
                        Column("id", Integer(), primary_key=True),
                        Column("model_id", String()),
                        Column("score", Integer()),
                    ],
                    create_table=True,
                )
            )
        })
        dt = catalog.get_datatable(ds, "eval")
        dt.store_chunk(
            pd.DataFrame([
                {"id": 1, "model_id": "m1", "score": 50},
                {"id": 2, "model_id": "m2", "score": 90},
            ])
        )

        registry = OpsSpecRegistry()
        registry.add_many([
            input_spec(
                metrics=[
                    OpsMetricTableSpec(
                        id="eval_metrics",
                        title="Eval metrics",
                        table="eval",
                        metric_source="eval",
                        primary_key_columns=["id"],
                        entity_links={"model": "model_id"},
                        primary_columns=[OpsColumn("id", "ID", "id")],
                        metric_columns=[OpsColumn("score", "Score", "score")],
                        best_metric_column="score",
                        default_sort=[("score", "desc")],
                    )
                ]
            )
        ])

        latest = latest_eval_metric_from_specs(registry, ds, catalog)
        assert latest is not None
        assert latest["metric_name"] == "score"
        assert latest["metric_value"] == pytest.approx(90.0)
        assert latest["model_id"] == "m2"

        scoped = latest_eval_metric_from_specs(registry, ds, catalog, model_id="m1")
        assert scoped is not None
        assert scoped["metric_value"] == pytest.approx(50.0)
        assert scoped["model_id"] == "m1"


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


def test_default_filters_serialize(ops_app):
    spec = input_spec(
        metrics=[
            OpsMetricTableSpec(
                id="filtered",
                title="Filtered",
                table="input",
                metric_source="input",
                primary_key_columns=["id"],
                entity_links={"item": "id"},
                primary_columns=[OpsColumn("id", "ID", "id", filterable=True)],
                metric_columns=[OpsColumn("value", "Value", "v")],
                filters=[OpsColumn("value_filter", "Value", "v", filterable=True)],
                default_filters=[OpsFilterRule(column_id="value_filter", operator="equal", value="a")],
            )
        ]
    )
    ops_app.add_specs([spec])
    client = TestClient(ops_app)

    res = client.get("/api/v1alpha3/pipelines/test_pipeline/ops-specs/catalog_tags")

    assert res.status_code == 200
    table = res.json()["metrics"][0]
    assert table["default_filters"] == [
        {"column_id": "value_filter", "operator": "equal", "value": "a"},
    ]


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

