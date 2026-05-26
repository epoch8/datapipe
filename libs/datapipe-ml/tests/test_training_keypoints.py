from __future__ import annotations

import pandas as pd
import pytest
from sqlalchemy import JSON, Column
from sqlalchemy.sql.sqltypes import DateTime, Integer, String

pytestmark = [pytest.mark.training, pytest.mark.torch]


def _require_datapipe_runtime():
    pytest.importorskip("tqdm")
    pytest.importorskip("datapipe")


def _make_catalog(dbconn):
    from datapipe.compute import Catalog, Table
    from datapipe.store.database import TableStoreDB

    return Catalog(
        {
            "keypoints_frozen_dataset": Table(
                store=TableStoreDB(
                    dbconn,
                    "keypoints_frozen_dataset",
                    [
                        Column("keypoints_frozen_dataset_id", String, primary_key=True),
                        Column("keypoints_frozen_dataset__created_at", DateTime),
                        Column("keypoints_frozen_dataset__images_count", Integer),
                    ],
                    True,
                )
            ),
            "keypoints_frozen_dataset__has__image_gt": Table(
                store=TableStoreDB(
                    dbconn,
                    "keypoints_frozen_dataset__has__image_gt",
                    [
                        Column("image_id", String, primary_key=True),
                        Column("keypoints_frozen_dataset_id", String, primary_key=True),
                        Column("subset_id", String, primary_key=True),
                        Column("image__image_path", String),
                        Column("bboxes", JSON),
                        Column("labels", JSON),
                        Column("keypoints", JSON),
                        Column("keypoints_visibility", JSON),
                        Column("flip_idx", JSON),
                    ],
                    True,
                )
            ),
        }
    )


def test_keypoints_yolov8_training_builds_pose_tables(base_datastore, dbconn, smoke_dataset, tmp_path):
    _require_datapipe_runtime()
    from datapipe.compute import Pipeline, build_compute

    from datapipe_ml.tasks.keypoints.train.yolov8 import (
        Train_YoloV8_KeypointsModel,
        YoloV8_TrainingConfig,
    )

    catalog = _make_catalog(dbconn)
    catalog.get_datatable(base_datastore, "keypoints_frozen_dataset").store_chunk(
        pd.DataFrame(
            {
                "keypoints_frozen_dataset_id": ["fd1"],
                "keypoints_frozen_dataset__created_at": [pd.Timestamp.utcnow().to_pydatetime()],
                "keypoints_frozen_dataset__images_count": [2],
            }
        )
    )
    gt = smoke_dataset.image_ground_truth_for_keypoints.iloc[:2].copy()
    gt["keypoints_frozen_dataset_id"] = "fd1"
    gt["subset_id"] = ["train", "val"]
    gt = pd.merge(gt, smoke_dataset.image.iloc[:2], on="image_id")
    catalog.get_datatable(base_datastore, "keypoints_frozen_dataset__has__image_gt").store_chunk(gt)

    steps = build_compute(
        base_datastore,
        catalog,
        Pipeline(
            [
                Train_YoloV8_KeypointsModel(
                    input__keypoints_frozen_dataset="keypoints_frozen_dataset",
                    input__keypoints_frozen_dataset__has__image_gt="keypoints_frozen_dataset__has__image_gt",
                    output__yolov8_train_config="keypoints_train_config",
                    output__keypoints_size_for_resize="keypoints_size_for_resize",
                    output__keypoints_frozen_dataset__class_names="keypoints_class_names",
                    output__keypoints_frozen_dataset__resized_image_file="keypoints_resized_image_file",
                    output__keypoints_frozen_dataset__yolo_txt="keypoints_yolo_txt",
                    output__keypoints_model="keypoints_model",
                    output__keypoints_model_is_trained_on_keypoints_frozen_dataset="keypoints_model_link",
                    working_dir=str(tmp_path),
                    yolov8_train_configs=[
                        YoloV8_TrainingConfig(model="yolo11n-pose.pt", imgsz=32, batch=1, epochs=1, device="cpu")
                    ],
                    primary_keys=["image_id"],
                    bbox_id__name=None,
                    create_table=True,
                )
            ]
        ),
    )

    assert len(steps) > 0
    model_columns = {column.name for column in base_datastore.get_table("keypoints_model").table_store.data_sql_schema}
    assert {
        "keypoints_model__pose_P",
        "keypoints_model__pose_R",
        "keypoints_model__pose_mAP50",
        "keypoints_model__pose_mAP50_95",
    }.issubset(model_columns)
    class_columns = {
        column.name for column in base_datastore.get_table("keypoints_class_names").table_store.data_sql_schema
    }
    assert {"kpt_shape", "flip_idx"}.issubset(class_columns)
