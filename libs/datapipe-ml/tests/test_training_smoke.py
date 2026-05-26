from __future__ import annotations

import pytest

from tests.helpers.training_smoke import (
    assert_model_artifact,
    detection_freeze_step,
    detection_train_step,
    detection_yolov5_train_step,
    keypoints_freeze_step,
    keypoints_train_step,
    make_runtime,
    run_pipeline,
    segmentation_freeze_step,
    segmentation_train_step,
)

pytestmark = [pytest.mark.slow, pytest.mark.training]


def test_training_smoke_sqlite_driver_supports_ci_full_outer_join(tmp_path):
    from tests.helpers.training_smoke import get_sqlite_dbconnstr

    assert get_sqlite_dbconnstr(tmp_path / "training_smoke.sqlite").startswith("sqlite")


@pytest.mark.torch
@pytest.mark.smoke
def test_yolov8_detection_training_smoke_cpu(tmp_path):
    runtime = make_runtime(tmp_path)
    run_pipeline(runtime, [detection_freeze_step(tmp_path), detection_train_step(tmp_path)])

    assert_model_artifact(
        runtime,
        "detection_model",
        "detection_model__type",
        "detection_model__model_path",
        "yolov8",
    )


@pytest.mark.torch
@pytest.mark.smoke
def test_yolov5_detection_training_smoke_cpu(tmp_path):
    runtime = make_runtime(tmp_path)
    run_pipeline(runtime, [detection_freeze_step(tmp_path), detection_yolov5_train_step(tmp_path)])

    assert_model_artifact(
        runtime,
        "detection_model",
        "detection_model__type",
        "detection_model__model_path",
        "yolov5",
    )


@pytest.mark.torch
@pytest.mark.smoke
def test_yolov8_segmentation_training_smoke_cpu(tmp_path):
    runtime = make_runtime(tmp_path)
    run_pipeline(runtime, [segmentation_freeze_step(tmp_path), segmentation_train_step(tmp_path)])

    assert_model_artifact(
        runtime,
        "segmentation_model",
        "segmentation_model__type",
        "segmentation_model__model_path",
        "yolov8",
    )


@pytest.mark.torch
@pytest.mark.smoke
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


@pytest.mark.tensorflow
@pytest.mark.smoke
def test_tensorflow_classification_training_smoke_cpu(tmp_path):
    from tests.helpers.training_smoke import (
        classification_freeze_step,
        classification_train_step,
    )

    runtime = make_runtime(tmp_path, include_classification_gt=True)
    run_pipeline(runtime, [classification_freeze_step(tmp_path), classification_train_step(tmp_path)])

    assert_model_artifact(
        runtime,
        "classification_model",
        "classification_model__type",
        "classification_model__model_path",
        "tf.keras",
    )
