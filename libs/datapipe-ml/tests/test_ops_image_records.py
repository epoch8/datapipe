import tempfile

import pandas as pd
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import JSON, Column, Integer, String

from datapipe.compute import Catalog, Pipeline, Table
from datapipe.store.database import DBConn, TableStoreDB

from datapipe_app import DatapipeAPI, OpsSpecRegistry
from datapipe_ml.ops_image_records import OpsImageRecordsSupport
from datapipe_app.ops_query import OpsQuery
from datapipe_app.specs import OpsColumn
from datapipe_ml.ops_specs import (
    DatapipeOpsSpec,
    OpsDataSpec,
    OpsFrozenDatasetSpec,
    OpsImageAnnotationSpec,
    OpsImageDataSpec,
    OpsImageRecordViewSpec,
    OpsModelSpec,
)


def image_spec(**overrides):
    values = {
        "id": "detection",
        "title": "Detection",
        "description": "Image detection ops.",
        "data": OpsDataSpec(
            tables=["images"],
            image_view=OpsImageDataSpec(
                image_table="images",
                image_primary_key_columns=["image_name"],
                image_url_column="image_url",
                ground_truth=None,
            ),
        ),
        "frozen_dataset": OpsFrozenDatasetSpec(
            table="frozen_snapshots",
            id_column="detection_frozen_dataset_id",
            created_at_column="created_at",
            record_view=OpsImageRecordViewSpec(
                table="frozen_has_image_gt",
                scope_column="detection_frozen_dataset_id",
                primary_key_columns=["detection_frozen_dataset_id", "image_name", "subset_id"],
                bboxes_column="bboxes",
                labels_column="labels",
            ),
            columns=[
                OpsColumn("detection_frozen_dataset_id", "Dataset", "detection_frozen_dataset_id", kind="link", link_to="frozen_dataset"),
                OpsColumn("created_at", "Frozen at", "created_at", kind="datetime"),
                OpsColumn("split", "Split", "split", kind="split"),
                OpsColumn("models", "Models", "models_count", kind="models_count"),
            ],
        ),
        "model": OpsModelSpec(
            table="models",
            id_column="model_id",
            prediction_view=None,
        ),
        "metrics": [],
    }
    values.update(overrides)
    return DatapipeOpsSpec(**values)


@pytest.fixture
def image_ops_app():
    with tempfile.TemporaryDirectory() as tmpdir:
        dbconn = DBConn(f"sqlite+pysqlite3:///{tmpdir}/store.sqlite")
        catalog = Catalog(
            {
                "images": Table(
                    store=TableStoreDB(
                        name="images",
                        dbconn=dbconn,
                        data_sql_schema=[
                            Column("image_name", String(), primary_key=True),
                            Column("image_url", String()),
                            Column("bboxes", JSON()),
                            Column("labels", JSON()),
                        ],
                        create_table=True,
                    )
                ),
                "frozen_snapshots": Table(
                    store=TableStoreDB(
                        name="frozen_snapshots",
                        dbconn=dbconn,
                        data_sql_schema=[
                            Column("detection_frozen_dataset_id", String(), primary_key=True),
                            Column("created_at", String()),
                        ],
                        create_table=True,
                    )
                ),
                "frozen_has_image_gt": Table(
                    store=TableStoreDB(
                        name="frozen_has_image_gt",
                        dbconn=dbconn,
                        data_sql_schema=[
                            Column("detection_frozen_dataset_id", String(), primary_key=True),
                            Column("image_name", String(), primary_key=True),
                            Column("subset_id", String(), primary_key=True),
                            Column("bboxes", JSON()),
                            Column("labels", JSON()),
                        ],
                        create_table=True,
                    )
                ),
                "models": Table(
                    store=TableStoreDB(
                        name="models",
                        dbconn=dbconn,
                        data_sql_schema=[
                            Column("model_id", String(), primary_key=True),
                        ],
                        create_table=True,
                    )
                ),
            }
        )
        from datapipe.datatable import DataStore

        ds = DataStore(dbconn, create_meta_table=True)
        images_dt = catalog.get_datatable(ds, "images")
        frozen_dt = catalog.get_datatable(ds, "frozen_has_image_gt")
        images_dt.store_chunk(
            pd.DataFrame(
                [
                    {
                        "image_name": "img_a.jpg",
                        "image_url": "https://example.com/a.jpg",
                        "bboxes": [[10, 10, 50, 50]],
                        "labels": ["cat"],
                    },
                    {
                        "image_name": "img_b.jpg",
                        "image_url": "https://example.com/b.jpg",
                        "bboxes": [],
                        "labels": [],
                    },
                ]
            )
        )
        frozen_dt.store_chunk(
            pd.DataFrame(
                [
                    {
                        "detection_frozen_dataset_id": "ds-1",
                        "image_name": "img_a.jpg",
                        "subset_id": "train",
                        "bboxes": [[10, 10, 50, 50]],
                        "labels": ["cat"],
                    },
                    {
                        "detection_frozen_dataset_id": "ds-1",
                        "image_name": "img_b.jpg",
                        "subset_id": "val",
                        "bboxes": [[1, 2, 3, 4]],
                        "labels": ["dog"],
                    },
                    {
                        "detection_frozen_dataset_id": "ds-2",
                        "image_name": "img_c.jpg",
                        "subset_id": "test",
                        "bboxes": [],
                        "labels": [],
                    },
                ]
            )
        )
        app = DatapipeAPI(ds, catalog, pipeline=Pipeline([]), pipeline_id="test_pipeline")
        app.add_specs([image_spec()])
        yield app


def test_ops_specs_metadata_has_image_flags(image_ops_app):
    client = TestClient(image_ops_app)
    res = client.get("/api/v1alpha3/pipelines/test_pipeline/ops-specs")
    assert res.status_code == 200
    spec = res.json()["specs"][0]
    assert spec["has_image"] is True
    assert spec["has_model_predictions"] is False


def test_get_spec_serializes_image_view(image_ops_app):
    client = TestClient(image_ops_app)
    res = client.get("/api/v1alpha3/pipelines/test_pipeline/ops-specs/detection")
    assert res.status_code == 200
    payload = res.json()
    assert payload["data"]["image_view"]["image_table"] == "images"
    assert payload["frozen_dataset"]["record_view"]["table"] == "frozen_has_image_gt"


def test_record_key_roundtrip():
    registry = OpsSpecRegistry()
    support = OpsImageRecordsSupport(registry, OpsQuery(None, None))
    pk = {"image_name": "img_a.jpg", "subset_id": "train"}
    encoded = support._encode_record_key(pk)
    decoded = support._decode_record_key(encoded)
    assert decoded == pk


def test_image_records_limit_clamped(image_ops_app):
    client = TestClient(image_ops_app)
    res = client.get("/api/v1alpha3/pipelines/test_pipeline/ops-specs/detection/image/records?limit=100&offset=0")
    assert res.status_code == 200
    payload = res.json()
    assert payload["limit"] == 100
    assert payload["total"] is None
    assert len(payload["rows"]) == 2
    assert payload["list_columns"] == ["image_name"]


def test_image_records_list_all_images_without_subset_rows(agent_env):
    with tempfile.TemporaryDirectory() as tmpdir:
        dbconn = DBConn(f"sqlite+pysqlite3:///{tmpdir}/store.sqlite")
        catalog = Catalog(
            {
                "s3_images": Table(
                    store=TableStoreDB(
                        name="s3_images",
                        dbconn=dbconn,
                        data_sql_schema=[
                            Column("image_name", String(), primary_key=True),
                            Column("image_url", String()),
                        ],
                        create_table=True,
                    )
                ),
                "image__subset": Table(
                    store=TableStoreDB(
                        name="image__subset",
                        dbconn=dbconn,
                        data_sql_schema=[
                            Column("image_name", String(), primary_key=True),
                            Column("subset_id", String()),
                        ],
                        create_table=True,
                    )
                ),
                "image__ground_truth": Table(
                    store=TableStoreDB(
                        name="image__ground_truth",
                        dbconn=dbconn,
                        data_sql_schema=[
                            Column("image_name", String(), primary_key=True),
                            Column("bboxes", JSON()),
                            Column("labels", JSON()),
                        ],
                        create_table=True,
                    )
                ),
            }
        )
        from datapipe.datatable import DataStore

        ds = DataStore(dbconn, create_meta_table=True)
        images_dt = catalog.get_datatable(ds, "s3_images")
        images_dt.store_chunk(
            pd.DataFrame(
                [
                    {"image_name": "img_a.jpg", "image_url": "https://example.com/a.jpg"},
                    {"image_name": "img_b.jpg", "image_url": "https://example.com/b.jpg"},
                ]
            )
        )
        spec = DatapipeOpsSpec(
            id="catalog",
            title="Catalog",
            description="Catalog images without subset split.",
            data=OpsDataSpec(
                tables=["s3_images", "image__subset", "image__ground_truth"],
                image_view=OpsImageDataSpec(
                    image_table="s3_images",
                    image_primary_key_columns=["image_name"],
                    image_url_column="image_url",
                    subset_table="image__subset",
                    subset_join_columns={"image_name": "image_name"},
                    subset_column="subset_id",
                    ground_truth=OpsImageAnnotationSpec(
                        table="image__ground_truth",
                        primary_key_columns=["image_name"],
                        bboxes_column="bboxes",
                        labels_column="labels",
                        join_columns={"image_name": "image_name"},
                    ),
                ),
            ),
            metrics=[],
        )
        app = DatapipeAPI(ds, catalog, pipeline=Pipeline([]), pipeline_id="test_pipeline")
        app.add_specs([spec])
        client = TestClient(app)

        res = client.get("/api/v1alpha3/pipelines/test_pipeline/ops-specs/catalog/image/records?limit=10&offset=0")
        assert res.status_code == 200
        payload = res.json()
        assert payload["list_columns"] == ["image_name"]
        assert len(payload["rows"]) == 2
        assert {row["pk"]["image_name"] for row in payload["rows"]} == {"img_a.jpg", "img_b.jpg"}
        assert all(row["subset"] is None for row in payload["rows"])
        assert all(row["bbox_count"] is None for row in payload["rows"])


def test_image_records_count_endpoint(image_ops_app):
    client = TestClient(image_ops_app)
    res = client.get("/api/v1alpha3/pipelines/test_pipeline/ops-specs/detection/image/records/count")
    assert res.status_code == 200
    assert res.json()["total"] == 2


def test_frozen_dataset_records_scoped_by_dataset_id(image_ops_app):
    client = TestClient(image_ops_app)
    res = client.get(
        "/api/v1alpha3/pipelines/test_pipeline/ops-specs/detection/frozen-datasets/ds-1/records?limit=10&offset=0&include_total=true"
    )
    assert res.status_code == 200
    payload = res.json()
    assert payload["total"] == 2
    subsets = {row["pk"]["subset_id"] for row in payload["rows"]}
    assert subsets == {"train", "val"}


def test_image_records_sort_by_image_name_desc(image_ops_app):
    client = TestClient(image_ops_app)
    res = client.get(
        "/api/v1alpha3/pipelines/test_pipeline/ops-specs/detection/image/records"
        "?limit=10&sort_by=image_name&sort_dir=desc"
    )
    assert res.status_code == 200
    names = [row["pk"]["image_name"] for row in res.json()["rows"]]
    assert names == ["img_b.jpg", "img_a.jpg"]


def test_frozen_dataset_records_sort_by_subset_id(image_ops_app):
    client = TestClient(image_ops_app)
    res = client.get(
        "/api/v1alpha3/pipelines/test_pipeline/ops-specs/detection/frozen-datasets/ds-1/records"
        "?limit=10&sort_by=subset_id&sort_dir=asc"
    )
    assert res.status_code == 200
    subsets = [row["pk"]["subset_id"] for row in res.json()["rows"]]
    assert subsets == ["train", "val"]


def test_frozen_dataset_record_detail_with_string_subset_id(image_ops_app):
    client = TestClient(image_ops_app)
    registry = image_ops_app.ops_specs
    support = OpsImageRecordsSupport(registry, OpsQuery(image_ops_app.ds, image_ops_app.catalog))
    record_key = support._encode_record_key(
        {"detection_frozen_dataset_id": "ds-1", "image_name": "img_a.jpg", "subset_id": "train"}
    )
    res = client.get(
        f"/api/v1alpha3/pipelines/test_pipeline/ops-specs/detection/frozen-datasets/ds-1/records/{record_key}"
    )
    assert res.status_code == 200
    payload = res.json()
    assert payload["subset"] == "train"
    assert payload["bbox_count"] == 1
    assert payload["pk"]["image_name"] == "img_a.jpg"
