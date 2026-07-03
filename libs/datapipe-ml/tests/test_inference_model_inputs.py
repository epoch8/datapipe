from __future__ import annotations

import pandas as pd
import pytest
from sqlalchemy import JSON, Column, Float
from sqlalchemy.sql.sqltypes import String


def _require_runtime():
    pytest.importorskip("datapipe")


def test_merge_inputs_on_keys_merges_filters_on_primary_keys():
    from datapipe_ml.inference.model_inputs import merge_inputs_on_keys

    images = pd.DataFrame(
        {
            "image_name": ["img-1", "img-2", "img-3"],
            "image_url": ["a.jpg", "b.jpg", "c.jpg"],
        }
    )
    unannotated = pd.DataFrame({"image_name": ["img-1", "img-3"]})

    resolved = merge_inputs_on_keys([images, unannotated], ["image_name"])

    assert resolved.to_dict("records") == [
        {"image_name": "img-1", "image_url": "a.jpg"},
        {"image_name": "img-3", "image_url": "c.jpg"},
    ]


def test_build_required_pipeline_inputs_marks_all_tables_as_required():
    _require_runtime()
    from datapipe.types import Required

    from datapipe_ml.inference.model_inputs import build_required_pipeline_inputs

    inputs = build_required_pipeline_inputs(["detection_model", "best_detection_model"])

    assert isinstance(inputs[0], Required)
    assert inputs[0].table == "detection_model"
    assert isinstance(inputs[1], Required)
    assert inputs[1].table == "best_detection_model"


def test_detection_inference_uses_model_list_filter(base_datastore, dbconn, monkeypatch):
    _require_runtime()
    from datapipe.compute import Catalog, Pipeline, Table, build_compute, run_steps
    from datapipe.store.database import TableStoreDB

    from datapipe_ml.tasks.detection.inference import Inference_DetectionModel

    seen_model_ids: list[str] = []

    def fake_inference(df__image, df__model, **kwargs):
        seen_model_ids.extend(df__model["detection_model_id"].tolist())
        return pd.DataFrame(
            columns=[
                "image_name",
                "detection_model_id",
                "bboxes",
                "labels",
                "prediction__detection_scores",
            ]
        )

    monkeypatch.setattr(
        "datapipe_ml.inference.bbox_pipeline.make_bbox_inference_func",
        lambda *args, **kwargs: fake_inference,
    )

    catalog = Catalog(
        {
            "image": Table(
                TableStoreDB(
                    dbconn,
                    "image",
                    [Column("image_name", String, primary_key=True), Column("image_url", String)],
                    True,
                )
            ),
            "detection_model": Table(
                TableStoreDB(
                    dbconn,
                    "detection_model",
                    [
                        Column("detection_model_id", String, primary_key=True),
                        Column("detection_model__input_size", JSON),
                        Column("detection_model__score_threshold", Float),
                        Column("detection_model__model_path", String),
                        Column("detection_model__type", String),
                        Column("detection_model__class_names", JSON),
                    ],
                    True,
                )
            ),
            "best_detection_model": Table(
                TableStoreDB(
                    dbconn,
                    "best_detection_model",
                    [Column("detection_model_id", String, primary_key=True)],
                    True,
                )
            ),
        }
    )
    catalog.get_datatable(base_datastore, "image").store_chunk(
        pd.DataFrame({"image_name": ["img-1"], "image_url": ["file:///tmp/img-1.jpg"]})
    )
    catalog.get_datatable(base_datastore, "detection_model").store_chunk(
        pd.DataFrame(
            {
                "detection_model_id": ["m1", "m2"],
                "detection_model__input_size": [[32, 32], [32, 32]],
                "detection_model__score_threshold": [0.1, 0.1],
                "detection_model__model_path": ["a.pt", "b.pt"],
                "detection_model__type": ["yolov8", "yolov8"],
                "detection_model__class_names": [["cat"], ["cat"]],
            }
        )
    )
    catalog.get_datatable(base_datastore, "best_detection_model").store_chunk(
        pd.DataFrame({"detection_model_id": ["m2"]})
    )

    steps = build_compute(
        base_datastore,
        catalog,
        Pipeline(
            [
                Inference_DetectionModel(
                    input__image="image",
                    input__detection_model=["detection_model", "best_detection_model"],
                    output__detection_prediction="detection_prediction",
                    primary_keys=["image_name"],
                    bbox_id__name=None,
                    create_table=True,
                    chunk_size=1,
                    image__image_path__name="image_url",
                )
            ]
        ),
    )
    run_steps(base_datastore, steps)

    assert seen_model_ids == ["m2"]


def test_classification_inference_uses_model_list_filter(base_datastore, dbconn, monkeypatch):
    _require_runtime()
    from datapipe.compute import Catalog, Pipeline, Table, build_compute, run_steps
    from datapipe.store.database import TableStoreDB

    from datapipe_ml.tasks.classification.inference import Inference_ClassificationModel

    seen_model_ids: list[str] = []

    def fake_classification_inference(df__image, df__classification_model, **kwargs):
        seen_model_ids.extend(df__classification_model["classification_model_id"].tolist())
        return pd.DataFrame(
            columns=[
                "image_name",
                "classification_model_id",
                "label",
                "prediction__top_n",
                "prediction__classification_score",
            ]
        )

    monkeypatch.setattr(
        "datapipe_ml.tasks.classification.inference.classification_inference",
        fake_classification_inference,
    )

    catalog = Catalog(
        {
            "image": Table(
                TableStoreDB(
                    dbconn,
                    "image",
                    [Column("image_name", String, primary_key=True), Column("image_url", String)],
                    True,
                )
            ),
            "classification_model": Table(
                TableStoreDB(
                    dbconn,
                    "classification_model",
                    [
                        Column("classification_model_id", String, primary_key=True),
                        Column("classification_model__input_size", JSON),
                        Column("classification_model__model_path", String),
                        Column("classification_model__type", String),
                        Column("classification_model__class_names", JSON),
                        Column("classification_model__preprocess_input_script_path", String),
                    ],
                    True,
                )
            ),
            "best_classification_model": Table(
                TableStoreDB(
                    dbconn,
                    "best_classification_model",
                    [Column("classification_model_id", String, primary_key=True)],
                    True,
                )
            ),
        }
    )
    catalog.get_datatable(base_datastore, "image").store_chunk(
        pd.DataFrame({"image_name": ["img-1"], "image_url": ["file:///tmp/img-1.jpg"]})
    )
    catalog.get_datatable(base_datastore, "classification_model").store_chunk(
        pd.DataFrame(
            {
                "classification_model_id": ["m1", "m2"],
                "classification_model__input_size": [[32, 32], [32, 32]],
                "classification_model__model_path": ["a", "b"],
                "classification_model__type": ["tf.keras", "tf.keras"],
                "classification_model__class_names": [["cat"], ["cat"]],
                "classification_model__preprocess_input_script_path": ["pre.py", "pre.py"],
            }
        )
    )
    catalog.get_datatable(base_datastore, "best_classification_model").store_chunk(
        pd.DataFrame({"classification_model_id": ["m1"]})
    )

    steps = build_compute(
        base_datastore,
        catalog,
        Pipeline(
            [
                Inference_ClassificationModel(
                    input__image="image",
                    input__classification_model=["classification_model", "best_classification_model"],
                    output__classification_prediction="classification_prediction",
                    primary_keys=["image_name"],
                    create_table=True,
                    chunk_size=1,
                    image__image_path__name="image_url",
                )
            ]
        ),
    )
    run_steps(base_datastore, steps)

    assert seen_model_ids == ["m1"]


def test_detection_inference_uses_image_list_filter(base_datastore, dbconn, monkeypatch):
    _require_runtime()
    from datapipe.compute import Catalog, Pipeline, Table, build_compute, run_steps
    from datapipe.store.database import TableStoreDB

    from datapipe_ml.tasks.detection.inference import Inference_DetectionModel

    seen_image_names: list[str] = []

    def fake_inference(df__image, df__model, **kwargs):
        seen_image_names.extend(df__image["image_name"].tolist())
        return pd.DataFrame(
            columns=[
                "image_name",
                "detection_model_id",
                "bboxes",
                "labels",
                "prediction__detection_scores",
            ]
        )

    monkeypatch.setattr(
        "datapipe_ml.inference.bbox_pipeline.make_bbox_inference_func",
        lambda *args, **kwargs: fake_inference,
    )

    catalog = Catalog(
        {
            "image": Table(
                TableStoreDB(
                    dbconn,
                    "image",
                    [Column("image_name", String, primary_key=True), Column("image_url", String)],
                    True,
                )
            ),
            "image_filter": Table(
                TableStoreDB(
                    dbconn,
                    "image_filter",
                    [Column("image_name", String, primary_key=True)],
                    True,
                )
            ),
            "detection_model": Table(
                TableStoreDB(
                    dbconn,
                    "detection_model",
                    [
                        Column("detection_model_id", String, primary_key=True),
                        Column("detection_model__input_size", JSON),
                        Column("detection_model__score_threshold", Float),
                        Column("detection_model__model_path", String),
                        Column("detection_model__type", String),
                        Column("detection_model__class_names", JSON),
                    ],
                    True,
                )
            ),
        }
    )
    catalog.get_datatable(base_datastore, "image").store_chunk(
        pd.DataFrame(
            {
                "image_name": ["img-1", "img-2", "img-3"],
                "image_url": [
                    "file:///tmp/img-1.jpg",
                    "file:///tmp/img-2.jpg",
                    "file:///tmp/img-3.jpg",
                ],
            }
        )
    )
    catalog.get_datatable(base_datastore, "image_filter").store_chunk(
        pd.DataFrame({"image_name": ["img-1", "img-3"]})
    )
    catalog.get_datatable(base_datastore, "detection_model").store_chunk(
        pd.DataFrame(
            {
                "detection_model_id": ["m1"],
                "detection_model__input_size": [[32, 32]],
                "detection_model__score_threshold": [0.1],
                "detection_model__model_path": ["a.pt"],
                "detection_model__type": ["yolov8"],
                "detection_model__class_names": [["cat"]],
            }
        )
    )

    steps = build_compute(
        base_datastore,
        catalog,
        Pipeline(
            [
                Inference_DetectionModel(
                    input__image=["image", "image_filter"],
                    input__detection_model="detection_model",
                    output__detection_prediction="detection_prediction",
                    primary_keys=["image_name"],
                    bbox_id__name=None,
                    create_table=True,
                    chunk_size=1,
                    image__image_path__name="image_url",
                )
            ]
        ),
    )
    run_steps(base_datastore, steps)

    assert seen_image_names == ["img-1", "img-3"]
