from __future__ import annotations

from pathlib import Path
from typing import Callable

import pytest

from tests.helpers.training_smoke import (
    Workdir,
    classification_freeze_step,
    classification_train_step,
    detection_freeze_step,
    detection_train_step,
    detection_yolov5_train_step,
    keypoints_freeze_step,
    keypoints_train_step,
    segmentation_freeze_step,
    segmentation_train_step,
)

SmokeStepsFactory = Callable[[Workdir, Path], list]

CLOUD_SMOKE_CASE_PARAMS = [
    pytest.param("yolov8_detection", marks=[pytest.mark.torch, pytest.mark.training, pytest.mark.slow]),
    pytest.param("yolov5_detection", marks=[pytest.mark.torch, pytest.mark.training, pytest.mark.slow]),
    pytest.param("yolov8_segmentation", marks=[pytest.mark.torch, pytest.mark.training, pytest.mark.slow]),
    pytest.param("yolov8_keypoints", marks=[pytest.mark.torch, pytest.mark.training, pytest.mark.slow]),
    pytest.param("tensorflow_classification", marks=[pytest.mark.tensorflow, pytest.mark.training, pytest.mark.slow]),
]

_CLOUD_SMOKE_CASES: dict[str, tuple[dict, SmokeStepsFactory]] = {
    "yolov8_detection": (
        dict(),
        lambda workdir, scratch: [
            detection_freeze_step(workdir),
            detection_train_step(
                workdir,
                local_scratch=scratch,
            ),
        ],
    ),
    "yolov5_detection": (
        dict(),
        lambda workdir, scratch: [
            detection_freeze_step(workdir),
            detection_yolov5_train_step(
                workdir,
                local_scratch=scratch,
            ),
        ],
    ),
    "yolov8_segmentation": (
        dict(),
        lambda workdir, scratch: [
            segmentation_freeze_step(workdir),
            segmentation_train_step(
                workdir,
                local_scratch=scratch,
            ),
        ],
    ),
    "yolov8_keypoints": (
        dict(include_keypoints_gt=True),
        lambda workdir, scratch: [
            keypoints_freeze_step(workdir),
            keypoints_train_step(
                workdir,
                local_scratch=scratch,
            ),
        ],
    ),
    "tensorflow_classification": (
        dict(include_classification_gt=True),
        lambda workdir, scratch: [
            classification_freeze_step(workdir),
            classification_train_step(
                workdir,
                local_scratch=scratch,
            ),
        ],
    ),
}

_CLOUD_SMOKE_ARTIFACTS: dict[str, tuple[str, str, str, str]] = {
    "yolov8_detection": ("detection_model", "detection_model__type", "detection_model__model_path", "yolov8"),
    "yolov5_detection": ("detection_model", "detection_model__type", "detection_model__model_path", "yolov5"),
    "yolov8_segmentation": (
        "segmentation_model",
        "segmentation_model__type",
        "segmentation_model__model_path",
        "yolov8",
    ),
    "yolov8_keypoints": (
        "keypoints_model",
        "keypoints_model__type",
        "keypoints_model__model_path",
        "yolov8_pose",
    ),
    "tensorflow_classification": (
        "classification_model",
        "classification_model__type",
        "classification_model__model_path",
        "tf.keras",
    ),
}

_CLOUD_SMOKE_STATUS_TABLES: dict[str, str] = {
    "yolov8_detection": "detection_training_status",
    "yolov5_detection": "detection_training_status",
    "yolov8_segmentation": "segmentation_training_status",
    "yolov8_keypoints": "keypoints_training_status",
    "tensorflow_classification": "classification_training_status",
}


def cloud_smoke_runtime_kwargs(case_id: str) -> dict:
    return _CLOUD_SMOKE_CASES[case_id][0]


def cloud_smoke_steps(case_id: str, workdir: Workdir, scratch: Path) -> list:
    return _CLOUD_SMOKE_CASES[case_id][1](workdir, scratch)


def cloud_smoke_artifact(case_id: str) -> tuple[str, str, str, str]:
    return _CLOUD_SMOKE_ARTIFACTS[case_id]


def cloud_smoke_status_table(case_id: str) -> str:
    return _CLOUD_SMOKE_STATUS_TABLES[case_id]
