from __future__ import annotations

import pytest

from tests.helpers.training_smoke import (
    assert_metrics_have_values,
    assert_model_artifact,
    assert_table_has_rows,
    classification_freeze_step,
    classification_inference_step,
    classification_metrics_step,
    classification_train_step,
    make_runtime,
    run_pipeline,
)


@pytest.mark.tensorflow
@pytest.mark.smoke
@pytest.mark.slow
@pytest.mark.training
def test_tensorflow_classification_training_smoke_cpu(tmp_path):
    runtime = make_runtime(tmp_path, include_classification_gt=True)
    run_pipeline(runtime, [classification_freeze_step(tmp_path), classification_train_step(tmp_path)])

    assert_model_artifact(
        runtime,
        "classification_model",
        "classification_model__type",
        "classification_model__model_path",
        "tf.keras",
    )


@pytest.mark.tensorflow
@pytest.mark.smoke
@pytest.mark.slow
@pytest.mark.training
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


@pytest.mark.tensorflow
@pytest.mark.slow
@pytest.mark.training
@pytest.mark.e2e
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
