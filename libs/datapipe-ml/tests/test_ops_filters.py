import json
import tempfile

import pandas as pd
import pytest
from datapipe.compute import Catalog, Pipeline, Table
from datapipe.datatable import DataStore
from datapipe.step.batch_transform import BatchTransform
from datapipe.store.database import DBConn, TableStoreDB
from fastapi.testclient import TestClient
from sqlalchemy import Column, Integer, String

from datapipe_app import DatapipeAPI
from datapipe_app.ops_filters import OpsFilterRule, parse_filter_rules
from datapipe_app.ops_query import OpsQuery
from datapipe_app.errors import OpsSpecValidationError
from datapipe_ml.ops_specs import DatapipeOpsSpec, OpsDataSpec
from datapipe_app.specs import OpsColumn, OpsMetricTableSpec


def metrics_spec(table_name: str = "metrics_rows") -> DatapipeOpsSpec:
    return DatapipeOpsSpec(
        id="filter_spec",
        title="Filter Spec",
        description="Filter tests",
        data=OpsDataSpec(tables=[table_name]),
        metrics=[
            OpsMetricTableSpec(
                id="model_metrics",
                title="Model metrics",
                table=table_name,
                metric_source="pipeline_model__metrics_on_subset",
                primary_key_columns=["model", "subset"],
                entity_links={"model": "model", "subset": "subset"},
                primary_columns=[
                    OpsColumn("model", "Model ID", "model", "link", filterable=True, link_to="model"),
                    OpsColumn("subset", "Subset", "subset", "chip", filterable=True),
                ],
                metric_columns=[OpsColumn("score", "Score", "score", "number")],
                filters=[
                    OpsColumn("subset_filter", "Subset", "subset", filterable=True),
                    OpsColumn("model_filter", "Model ID", "model", filterable=True),
                ],
                best_metric_column="score",
                default_sort=[("model", "asc")],
            )
        ],
    )


@pytest.fixture
def filter_app(agent_env):
    with tempfile.TemporaryDirectory() as tmpdir:
        dbconn = DBConn(f"sqlite+pysqlite3:///{tmpdir}/store.sqlite")
        catalog = Catalog(
            {
                "metrics_rows": Table(
                    store=TableStoreDB(
                        name="metrics_rows",
                        dbconn=dbconn,
                        data_sql_schema=[
                            Column("id", Integer(), primary_key=True),
                            Column("model", String()),
                            Column("subset", String()),
                            Column("score", Integer()),
                        ],
                        create_table=True,
                    )
                )
            }
        )
        pipeline = Pipeline([BatchTransform(lambda df: df, inputs=["metrics_rows"], outputs=["metrics_rows"])])
        ds = DataStore(dbconn, create_meta_table=True)
        dt = catalog.get_datatable(ds, "metrics_rows")
        dt.store_chunk(
            pd.DataFrame(
                [
                    {"id": 1, "model": "cat_dog_yolo_smoke", "subset": "train", "score": 80},
                    {"id": 2, "model": "cat_dog_yolo_smoke", "subset": "val", "score": 70},
                    {"id": 3, "model": "other_model", "subset": "val", "score": 50},
                    {"id": 4, "model": "empty_model", "subset": "none", "score": 10},
                    {"id": 5, "model": None, "subset": "test", "score": 20},
                ]
            )
        )
        app = DatapipeAPI(ds, catalog, pipeline, pipeline_id="test_pipeline")
        app.add_specs([metrics_spec()])
        yield app


def _rows(client: TestClient, **params):
    return client.get(
        "/api/v1alpha3/pipelines/test_pipeline/ops-specs/filter_spec/metrics/model_metrics/rows",
        params=params,
    )


def test_contains_filter(filter_app):
    client = TestClient(filter_app)
    filters = json.dumps([{"column_id": "model", "operator": "contains", "value": "cat_dog"}])
    res = _rows(client, filters=filters)
    assert res.status_code == 200
    models = {row["model"] for row in res.json()["rows"]}
    assert models == {"cat_dog_yolo_smoke"}


def test_not_contains_filter(filter_app):
    client = TestClient(filter_app)
    filters = json.dumps([{"column_id": "model", "operator": "not_contains", "value": "cat_dog"}])
    res = _rows(client, filters=filters)
    assert res.status_code == 200
    models = {row["model"] for row in res.json()["rows"] if row["model"]}
    assert "cat_dog_yolo_smoke" not in models


def test_equal_and_not_equal_filters(filter_app):
    client = TestClient(filter_app)
    equal = json.dumps([{"column_id": "subset", "operator": "equal", "value": "train"}])
    res = _rows(client, filters=equal)
    assert res.status_code == 200
    assert all(row["subset"] == "train" for row in res.json()["rows"])

    not_equal = json.dumps([{"column_id": "subset", "operator": "not_equal", "value": "val"}])
    res = _rows(client, filters=not_equal)
    assert res.status_code == 200
    assert all(row["subset"] != "val" for row in res.json()["rows"])


def test_is_empty_filter(filter_app):
    client = TestClient(filter_app)
    filters = json.dumps([{"column_id": "model", "operator": "is_empty"}])
    res = _rows(client, filters=filters)
    assert res.status_code == 200
    assert any(row["model"] in {None, ""} for row in res.json()["rows"])


def test_filter_mode_or(filter_app):
    client = TestClient(filter_app)
    filters = json.dumps(
        [
            {"column_id": "subset", "operator": "equal", "value": "train"},
            {"column_id": "subset", "operator": "equal", "value": "test"},
        ]
    )
    res = _rows(client, filters=filters, filter_mode="or")
    assert res.status_code == 200
    subsets = {row["subset"] for row in res.json()["rows"]}
    assert subsets == {"train", "test"}


def test_filter_mode_and(filter_app):
    client = TestClient(filter_app)
    filters = json.dumps(
        [
            {"column_id": "model", "operator": "contains", "value": "cat_dog"},
            {"column_id": "subset", "operator": "equal", "value": "val"},
        ]
    )
    res = _rows(client, filters=filters, filter_mode="and")
    assert res.status_code == 200
    payload = res.json()
    assert payload["total"] == 1
    assert payload["rows"][0]["subset"] == "val"


def test_regex_filter_sqlite(filter_app):
    client = TestClient(filter_app)
    filters = json.dumps([{"column_id": "model", "operator": "regex", "value": "^cat_dog.*"}])
    res = _rows(client, filters=filters)
    assert res.status_code == 200
    assert all(str(row["model"]).startswith("cat_dog") for row in res.json()["rows"])


def test_unknown_column_is_skipped(filter_app):
    client = TestClient(filter_app)
    filters = json.dumps([{"column_id": "missing", "operator": "equal", "value": "x"}])
    res = _rows(client, filters=filters)
    assert res.status_code == 200
    assert res.json()["total"] == 5


def test_shared_filter_skipped_when_column_absent_from_table(filter_app):
    """Tag filter from a sibling metric table must not 400 on model_metrics."""
    query = OpsQuery(filter_app.ds, filter_app.catalog)
    columns = metrics_spec().metrics[0].primary_columns + metrics_spec().metrics[0].filters
    rows, total = query.rows(
        "metrics_rows",
        allowed_columns=columns,
        filter_rules=[
            OpsFilterRule(column_id="subset", operator="equal", value="val"),
            OpsFilterRule(column_id="tag_id", operator="equal", value="night"),
        ],
        filter_mode="and",
    )
    assert total == 2
    assert all(row["subset"] == "val" for row in rows)


def test_unknown_operator_returns_400():
    with pytest.raises(OpsSpecValidationError):
        parse_filter_rules(json.dumps([{"column_id": "model", "operator": "greater_than", "value": "1"}]))


def test_legacy_model_subset_params(filter_app):
    client = TestClient(filter_app)
    res = _rows(client, subset=["val"])
    assert res.status_code == 200
    assert all(row["subset"] == "val" for row in res.json()["rows"])


def test_search_and_filters_combine(filter_app):
    client = TestClient(filter_app)
    filters = json.dumps([{"column_id": "subset", "operator": "equal", "value": "val"}])
    res = _rows(client, filters=filters, search="cat_dog")
    assert res.status_code == 200
    payload = res.json()
    assert payload["total"] == 1
    assert payload["rows"][0]["model"] == "cat_dog_yolo_smoke"


def test_ops_query_direct_non_filterable_column(filter_app):
    query = OpsQuery(filter_app.ds, filter_app.catalog)
    with pytest.raises(OpsSpecValidationError, match="not configured"):
        query.rows(
            "metrics_rows",
            allowed_columns=[
                OpsColumn("model", "Model ID", "model", filterable=True),
                OpsColumn("score", "Score", "score", "number", filterable=False),
            ],
            filter_rules=[OpsFilterRule(column_id="score", operator="equal", value="1")],
        )
