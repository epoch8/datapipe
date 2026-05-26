from __future__ import annotations

import os

import pytest

pytestmark = [
    pytest.mark.e2e_examples,
    pytest.mark.e2e_template,
    pytest.mark.service_e2e,
    pytest.mark.training,
    pytest.mark.torch,
]


def _require_service_env() -> None:
    missing = [
        name
        for name in [
            "DB_URL",
            "LABEL_STUDIO_URL",
            "LABEL_STUDIO_API_KEY",
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
            "S3_BUCKET",
        ]
        if not os.environ.get(name)
    ]
    if missing:
        pytest.skip(f"service e2e env is not configured: {', '.join(missing)}")


def test_detection_template_service_pipeline_smoke(tmp_path):
    from examples.e2e_template.common import ServiceSettings
    from examples.e2e_template.image_detection.app import build_pipeline as build_detection_pipeline
    from tests.helpers.training_smoke import (
        assert_metrics_have_values,
        assert_table_has_rows,
        detection_freeze_step,
        detection_inference_step,
        detection_metrics_step,
        detection_train_step,
        make_runtime,
        run_pipeline,
    )

    _require_service_env()
    settings = ServiceSettings.from_env()
    pipeline = build_detection_pipeline(settings, include_training=True)
    assert len(pipeline.steps) > 0

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


def test_keypoints_template_service_pipeline_smoke(tmp_path):
    from examples.e2e_template.common import ServiceSettings
    from examples.e2e_template.image_keypoints.app import build_pipeline as build_keypoints_pipeline
    from tests.helpers.training_smoke import (
        assert_metrics_have_values,
        assert_table_has_rows,
        keypoints_freeze_step,
        keypoints_inference_step,
        keypoints_metrics_step,
        keypoints_train_step,
        make_runtime,
        run_pipeline,
    )

    _require_service_env()
    settings = ServiceSettings.from_env()
    pipeline = build_keypoints_pipeline(settings, include_training=True)
    assert len(pipeline.steps) > 0

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

