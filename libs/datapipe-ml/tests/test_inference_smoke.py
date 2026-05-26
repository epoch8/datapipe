from __future__ import annotations

import pytest

from tests.helpers.training_smoke import (
    assert_table_has_rows,
    classification_freeze_step,
    classification_inference_step,
    classification_train_step,
    detection_freeze_step,
    detection_inference_step,
    detection_train_step,
    keypoints_freeze_step,
    keypoints_inference_step,
    keypoints_train_step,
    make_runtime,
    run_pipeline,
    segmentation_freeze_step,
    segmentation_inference_step,
    segmentation_train_step,
)

pytestmark = [pytest.mark.slow, pytest.mark.training]


@pytest.mark.torch
@pytest.mark.smoke
def test_detection_inference_smoke_cpu(tmp_path):
    runtime = make_runtime(tmp_path)
    run_pipeline(
        runtime,
        [
            detection_freeze_step(tmp_path),
            detection_train_step(tmp_path),
            detection_inference_step(),
        ],
    )

    df = assert_table_has_rows(runtime, "detection_prediction")
    assert {"image_id", "bboxes", "labels", "prediction__detection_scores"}.issubset(df.columns)


@pytest.mark.torch
@pytest.mark.smoke
def test_segmentation_inference_smoke_cpu(tmp_path):
    runtime = make_runtime(tmp_path)
    run_pipeline(
        runtime,
        [
            segmentation_freeze_step(tmp_path),
            segmentation_train_step(tmp_path),
            segmentation_inference_step(),
        ],
    )

    df = assert_table_has_rows(runtime, "segmentation_prediction")
    assert {"image_id", "bboxes", "labels", "masks", "prediction__detection_scores"}.issubset(df.columns)


@pytest.mark.torch
@pytest.mark.smoke
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


@pytest.mark.tensorflow
@pytest.mark.smoke
def test_classification_inference_smoke_cpu(tmp_path):
    runtime = make_runtime(tmp_path, include_classification_gt=True)
    run_pipeline(
        runtime,
        [
            classification_freeze_step(tmp_path),
            classification_train_step(tmp_path),
            classification_inference_step(),
        ],
    )

    df = assert_table_has_rows(runtime, "classification_prediction")
    assert {"image_id", "label", "prediction__classification_score"}.issubset(df.columns)
