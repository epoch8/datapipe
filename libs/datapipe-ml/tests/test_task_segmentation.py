from __future__ import annotations

import pytest

from tests.helpers.training_smoke import (
    assert_model_artifact,
    assert_table_has_rows,
    make_runtime,
    run_pipeline,
    segmentation_freeze_step,
    segmentation_inference_step,
    segmentation_train_step,
)


@pytest.mark.torch
@pytest.mark.smoke
@pytest.mark.slow
@pytest.mark.training
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
@pytest.mark.slow
@pytest.mark.training
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
@pytest.mark.slow
@pytest.mark.training
@pytest.mark.e2e
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
