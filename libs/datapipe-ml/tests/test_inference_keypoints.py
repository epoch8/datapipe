from __future__ import annotations

import pandas as pd
import pytest
from sqlalchemy import JSON, Column, Float
from sqlalchemy.sql.sqltypes import String


def _require_runtime():
    pytest.importorskip("datapipe")
    pytest.importorskip("cv_pipeliner")


class _FakePipelineModelSpec:
    def __init__(self, detection_model_spec, classification_model_spec):
        self.detection_model_spec = detection_model_spec
        self.classification_model_spec = classification_model_spec

    def load_pipeline_inferencer(self):
        return _FakeInferencer()


class _FakeInferencer:
    def predict(self, images_data, detection_score_threshold, disable_tqdm=True, batch_size_default=1):
        from cv_pipeliner.core.data import BboxData, ImageData

        return [
            ImageData(
                image_path=image_data.image_path,
                bboxes_data=[
                    BboxData(
                        xmin=1,
                        ymin=2,
                        xmax=10,
                        ymax=12,
                        label="cat",
                        keypoints=[[1, 2], [10, 2], [10, 12], [1, 12]],
                        detection_score=0.9,
                        keypoints_scores=[0.9, 0.8, 0.7, 0.6],
                    )
                ],
            )
            for image_data in images_data
        ]


def _make_catalog(dbconn):
    from datapipe.compute import Catalog, Table
    from datapipe.store.database import TableStoreDB

    return Catalog(
        {
            "image": Table(
                store=TableStoreDB(
                    dbconn,
                    "image",
                    [Column("image_id", String, primary_key=True), Column("image__image_path", String)],
                    True,
                )
            ),
            "keypoints_model": Table(
                store=TableStoreDB(
                    dbconn,
                    "keypoints_model",
                    [
                        Column("keypoints_model_id", String, primary_key=True),
                        Column("keypoints_model__input_size", JSON),
                        Column("keypoints_model__score_threshold", Float),
                        Column("keypoints_model__model_path", String),
                        Column("keypoints_model__type", String),
                        Column("keypoints_model__class_names", JSON),
                    ],
                    True,
                )
            ),
        }
    )


def test_keypoints_inference_outputs_keypoints_and_scores(base_datastore, dbconn, smoke_dataset, monkeypatch):
    _require_runtime()
    from datapipe.compute import Pipeline, build_compute, run_steps

    from datapipe_ml.inference import bbox_pipeline as bbox_pipeline_module
    from datapipe_ml.tasks.keypoints.inference import Inference_KeypointsModel

    monkeypatch.setattr(bbox_pipeline_module, "PipelineModelSpec", _FakePipelineModelSpec)
    catalog = _make_catalog(dbconn)
    catalog.get_datatable(base_datastore, "image").store_chunk(smoke_dataset.image.iloc[:1].copy())
    catalog.get_datatable(base_datastore, "keypoints_model").store_chunk(
        pd.DataFrame(
            {
                "keypoints_model_id": ["m1"],
                "keypoints_model__input_size": [[32, 32]],
                "keypoints_model__score_threshold": [0.1],
                "keypoints_model__model_path": ["unused.pt"],
                "keypoints_model__type": ["yolov8_pose"],
                "keypoints_model__class_names": [["cat"]],
            }
        )
    )

    steps = build_compute(
        base_datastore,
        catalog,
        Pipeline(
            [
                Inference_KeypointsModel(
                    input__image="image",
                    input__keypoints_model="keypoints_model",
                    output__keypoints_prediction="keypoints_prediction",
                    primary_keys=["image_id"],
                    bbox_id__name=None,
                    create_table=True,
                    chunk_size=1,
                )
            ]
        ),
    )
    run_steps(base_datastore, steps)

    df = base_datastore.get_table("keypoints_prediction").get_data()
    assert len(df) == 1
    assert df["bboxes"].iloc[0] == [[1, 2, 10, 12]]
    assert df["keypoints"].iloc[0] == [[[1, 2], [10, 2], [10, 12], [1, 12]]]
    assert df["prediction__keypoints_scores"].iloc[0] == [[0.9, 0.8, 0.7, 0.6]]
