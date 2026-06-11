from __future__ import annotations

import pytest

from tests.helpers.cloud_storage import assert_model_path_under_working_dir
from tests.helpers.training_smoke import (
    assert_model_artifact,
    assert_training_uses_architecture_label,
    detection_freeze_step,
    detection_train_step_with_local_checkpoint,
    detection_yolov5_train_step,
    keypoints_freeze_step,
    keypoints_train_step_with_local_checkpoint,
    make_cloud_runtime,
    make_runtime,
    run_pipeline,
    segmentation_freeze_step,
    segmentation_train_step_with_local_checkpoint,
)

pytestmark = [pytest.mark.torch, pytest.mark.smoke, pytest.mark.slow, pytest.mark.training]


def test_yolov8_detection_training_with_local_checkpoint_uses_architecture_label(tmp_path):
    runtime = make_runtime(tmp_path)
    run_pipeline(
        runtime,
        [detection_freeze_step(tmp_path), detection_train_step_with_local_checkpoint(tmp_path, "yolo11n.pt")],
    )

    assert_model_artifact(
        runtime,
        "detection_model",
        "detection_model__type",
        "detection_model__model_path",
        "yolov8",
    )
    assert_training_uses_architecture_label(
        runtime,
        table_name="detection_model",
        model_id_column="detection_model_id",
        model_path_column="detection_model__model_path",
        architecture="yolo11n",
    )


def test_yolov8_segmentation_training_with_local_checkpoint_uses_architecture_label(tmp_path):
    runtime = make_runtime(tmp_path)
    run_pipeline(
        runtime,
        [
            segmentation_freeze_step(tmp_path),
            segmentation_train_step_with_local_checkpoint(tmp_path, "yolov8n-seg.pt"),
        ],
    )

    assert_model_artifact(
        runtime,
        "segmentation_model",
        "segmentation_model__type",
        "segmentation_model__model_path",
        "yolov8",
    )
    assert_training_uses_architecture_label(
        runtime,
        table_name="segmentation_model",
        model_id_column="segmentation_model_id",
        model_path_column="segmentation_model__model_path",
        architecture="yolov8n-seg",
    )


def test_yolov8_keypoints_training_with_local_checkpoint_uses_architecture_label(tmp_path):
    runtime = make_runtime(tmp_path, include_keypoints_gt=True)
    run_pipeline(
        runtime,
        [
            keypoints_freeze_step(tmp_path),
            keypoints_train_step_with_local_checkpoint(tmp_path, "yolo11n-pose.pt"),
        ],
    )

    assert_model_artifact(
        runtime,
        "keypoints_model",
        "keypoints_model__type",
        "keypoints_model__model_path",
        "yolov8_pose",
    )
    assert_training_uses_architecture_label(
        runtime,
        table_name="keypoints_model",
        model_id_column="keypoints_model_id",
        model_path_column="keypoints_model__model_path",
        architecture="yolo11n-pose",
    )


def test_yolov5_detection_training_uses_cfg_architecture_label(tmp_path):
    runtime = make_runtime(tmp_path)
    run_pipeline(runtime, [detection_freeze_step(tmp_path), detection_yolov5_train_step(tmp_path)])

    assert_model_artifact(
        runtime,
        "detection_model",
        "detection_model__type",
        "detection_model__model_path",
        "yolov5",
    )
    assert_training_uses_architecture_label(
        runtime,
        table_name="detection_model",
        model_id_column="detection_model_id",
        model_path_column="detection_model__model_path",
        architecture="yolov5n",
        checkpoint_alias="__missing__",
    )


@pytest.mark.cloud_storage
@pytest.mark.service_e2e
def test_yolov8_detection_cloud_working_dir_with_cloud_preset_checkpoint(tmp_path):
    runtime, workdir = make_cloud_runtime(tmp_path, "local-checkpoint-yolov8-detection")
    run_pipeline(
        runtime,
        [
            detection_freeze_step(workdir),
            detection_train_step_with_local_checkpoint(workdir, "yolo11n.pt", local_scratch=tmp_path),
        ],
    )

    assert_model_artifact(
        runtime,
        "detection_model",
        "detection_model__type",
        "detection_model__model_path",
        "yolov8",
    )
    assert_training_uses_architecture_label(
        runtime,
        table_name="detection_model",
        model_id_column="detection_model_id",
        model_path_column="detection_model__model_path",
        architecture="yolo11n",
    )
    df_model = runtime.ds.get_table("detection_model").get_data()
    assert_model_path_under_working_dir(str(df_model["detection_model__model_path"].iloc[0]), workdir)


@pytest.mark.cloud_storage
@pytest.mark.service_e2e
def test_yolov8_segmentation_cloud_working_dir_with_cloud_preset_checkpoint(tmp_path):
    runtime, workdir = make_cloud_runtime(tmp_path, "local-checkpoint-yolov8-segmentation")
    run_pipeline(
        runtime,
        [
            segmentation_freeze_step(workdir),
            segmentation_train_step_with_local_checkpoint(workdir, "yolov8n-seg.pt", local_scratch=tmp_path),
        ],
    )

    assert_model_artifact(
        runtime,
        "segmentation_model",
        "segmentation_model__type",
        "segmentation_model__model_path",
        "yolov8",
    )
    assert_training_uses_architecture_label(
        runtime,
        table_name="segmentation_model",
        model_id_column="segmentation_model_id",
        model_path_column="segmentation_model__model_path",
        architecture="yolov8n-seg",
    )
    df_model = runtime.ds.get_table("segmentation_model").get_data()
    assert_model_path_under_working_dir(str(df_model["segmentation_model__model_path"].iloc[0]), workdir)


@pytest.mark.cloud_storage
@pytest.mark.service_e2e
def test_yolov8_keypoints_cloud_working_dir_with_cloud_preset_checkpoint(tmp_path):
    runtime, workdir = make_cloud_runtime(tmp_path, "local-checkpoint-yolov8-keypoints", include_keypoints_gt=True)
    run_pipeline(
        runtime,
        [
            keypoints_freeze_step(workdir),
            keypoints_train_step_with_local_checkpoint(workdir, "yolo11n-pose.pt", local_scratch=tmp_path),
        ],
    )

    assert_model_artifact(
        runtime,
        "keypoints_model",
        "keypoints_model__type",
        "keypoints_model__model_path",
        "yolov8_pose",
    )
    assert_training_uses_architecture_label(
        runtime,
        table_name="keypoints_model",
        model_id_column="keypoints_model_id",
        model_path_column="keypoints_model__model_path",
        architecture="yolo11n-pose",
    )
    df_model = runtime.ds.get_table("keypoints_model").get_data()
    assert_model_path_under_working_dir(str(df_model["keypoints_model__model_path"].iloc[0]), workdir)
