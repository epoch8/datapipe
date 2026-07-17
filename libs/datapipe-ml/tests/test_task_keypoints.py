from __future__ import annotations

import pandas as pd
import pytest
from sqlalchemy import JSON, Column
from sqlalchemy.sql.sqltypes import DateTime, Integer, String

from tests.helpers.training_smoke import (
    assert_completed_training_status_with_manifest,
    assert_metrics_have_values,
    assert_model_artifact,
    assert_table_has_rows,
    keypoints_freeze_step,
    keypoints_inference_step,
    keypoints_metrics_step,
    keypoints_train_step,
    make_runtime,
    run_pipeline,
    SMOKE_DEVICE,
)


@pytest.mark.torch
@pytest.mark.smoke
@pytest.mark.slow
@pytest.mark.training
def test_yolov8_keypoints_training_smoke_cpu(tmp_path):
    runtime = make_runtime(tmp_path, include_keypoints_gt=True)
    run_pipeline(runtime, [keypoints_freeze_step(tmp_path), keypoints_train_step(tmp_path)])

    assert_model_artifact(
        runtime,
        "keypoints_model",
        "keypoints_model__type",
        "keypoints_model__model_path",
        "yolov8_pose",
    )
    assert_completed_training_status_with_manifest(runtime, "keypoints_training_status")


@pytest.mark.torch
@pytest.mark.smoke
@pytest.mark.slow
@pytest.mark.training
def test_keypoints_inference_smoke_cpu(tmp_path):
    runtime = make_runtime(tmp_path, include_keypoints_gt=True)
    run_pipeline(
        runtime,
        [
            keypoints_freeze_step(tmp_path),
            keypoints_train_step(tmp_path),
            keypoints_inference_step(),
        ],
    )

    df = assert_table_has_rows(runtime, "keypoints_prediction")
    assert {
        "image_id",
        "bboxes",
        "labels",
        "keypoints",
        "prediction__detection_scores",
        "prediction__keypoints_scores",
    }.issubset(df.columns)


@pytest.mark.torch
@pytest.mark.slow
@pytest.mark.training
@pytest.mark.e2e
def test_keypoints_pipeline_e2e_smoke_cpu(tmp_path):
    runtime = make_runtime(tmp_path, include_keypoints_gt=True)
    run_pipeline(
        runtime,
        [
            keypoints_freeze_step(tmp_path),
            keypoints_train_step(tmp_path),
            keypoints_inference_step(),
            keypoints_metrics_step(),
        ],
    )

    assert_table_has_rows(runtime, "keypoints_prediction")
    assert_metrics_have_values(
        runtime,
        "keypoints_metrics_on_subset",
        ["calc__support", "calc__TP", "calc__FP", "calc__FN"],
    )


@pytest.mark.torch
@pytest.mark.training
def test_keypoints_yolov8_training_builds_pose_tables(base_datastore, dbconn, smoke_dataset, tmp_path):
    pytest.importorskip("tqdm")
    pytest.importorskip("datapipe")

    from datapipe.compute import Catalog, Pipeline, Table, build_compute
    from datapipe.store.database import TableStoreDB
    from datapipe_ml.tasks.keypoints.train.yolov8 import (
        Train_YoloV8_KeypointsModel,
        YoloV8_TrainingConfig,
    )

    catalog = Catalog(
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
                    output__model_keypoints_size_for_resize="model_keypoints_size_for_resize",
                    output__keypoints_size_for_resize="keypoints_size_for_resize",
                    output__keypoints_frozen_dataset__class_names="keypoints_class_names",
                    output__keypoints_frozen_dataset__resized_image_file="keypoints_resized_image_file",
                    output__keypoints_frozen_dataset__yolo_txt="keypoints_yolo_txt",
                    output__keypoints_model="keypoints_model",
                    output__keypoints_model_is_trained_on_keypoints_frozen_dataset="keypoints_model_link",
                    output__training_status="keypoints_training_status",
                    working_dir=str(tmp_path),
                    yolov8_train_configs=[
                        YoloV8_TrainingConfig(model="yolo11n-pose.pt", imgsz=32, batch=1, epochs=1, device=SMOKE_DEVICE)
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
