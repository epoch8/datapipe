from __future__ import annotations

import pytest

from tests.helpers.training_smoke import (
    assert_metrics_have_values,
    assert_model_artifact,
    assert_table_has_rows,
    detection_freeze_step,
    detection_inference_step,
    detection_metrics_step,
    detection_train_step,
    detection_yolov5_train_step,
    make_runtime,
    run_pipeline,
)


@pytest.mark.slow
@pytest.mark.training
def test_training_smoke_sqlite_driver_supports_ci_full_outer_join(tmp_path):
    from tests.helpers.training_smoke import get_sqlite_dbconnstr

    assert get_sqlite_dbconnstr(tmp_path / "training_smoke.sqlite").startswith("sqlite")


@pytest.mark.torch
@pytest.mark.smoke
@pytest.mark.slow
@pytest.mark.training
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
@pytest.mark.slow
@pytest.mark.training
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
@pytest.mark.slow
@pytest.mark.training
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
@pytest.mark.slow
@pytest.mark.training
@pytest.mark.e2e
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
