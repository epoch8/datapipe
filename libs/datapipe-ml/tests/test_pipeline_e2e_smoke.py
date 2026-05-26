from __future__ import annotations

import pytest

from tests.helpers.training_smoke import (
    assert_metrics_have_values,
    assert_table_has_rows,
    classification_freeze_step,
    classification_inference_step,
    classification_metrics_step,
    classification_train_step,
    detection_freeze_step,
    detection_inference_step,
    detection_metrics_step,
    detection_train_step,
    keypoints_freeze_step,
    keypoints_inference_step,
    keypoints_metrics_step,
    keypoints_train_step,
    make_runtime,
    run_pipeline,
    segmentation_freeze_step,
    segmentation_inference_step,
    segmentation_train_step,
)

pytestmark = [pytest.mark.slow, pytest.mark.training, pytest.mark.e2e]


@pytest.mark.torch
def test_detection_pipeline_e2e_smoke_cpu(tmp_path):
    runtime = make_runtime(tmp_path)
    run_pipeline(
        runtime,
        [
            detection_freeze_step(tmp_path),
            detection_train_step(tmp_path),
            detection_inference_step(),
            detection_metrics_step(),
        ],
    )

    assert_table_has_rows(runtime, "detection_prediction")
    assert_metrics_have_values(
        runtime,
        "detection_metrics_on_subset",
        ["calc__support", "calc__TP", "calc__FP", "calc__FN"],
    )


@pytest.mark.torch
def test_segmentation_pipeline_e2e_smoke_cpu(tmp_path):
    runtime = make_runtime(tmp_path)
    run_pipeline(
        runtime,
        [
            segmentation_freeze_step(tmp_path),
            segmentation_train_step(tmp_path),
            segmentation_inference_step(),
        ],
    )

    assert_table_has_rows(runtime, "segmentation_prediction")


@pytest.mark.torch
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


@pytest.mark.tensorflow
def test_classification_pipeline_e2e_smoke_cpu(tmp_path):
    runtime = make_runtime(tmp_path, include_classification_gt=True)
    run_pipeline(
        runtime,
        [
            classification_freeze_step(tmp_path),
            classification_train_step(tmp_path),
            classification_inference_step(),
            classification_metrics_step(),
        ],
    )

    assert_table_has_rows(runtime, "classification_prediction")
    assert_metrics_have_values(
        runtime,
        "classification_metrics_on_subset",
        ["calc__support", "calc__images_support", "calc__accuracy"],
    )
