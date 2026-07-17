from __future__ import annotations

from pathlib import Path

import pytest

from datapipe_ml.frameworks.yolo.checkpoint_label import (
    build_yolo_train_config_summary,
    resolve_yolo_model_label,
    resolve_yolo_model_label_from_params,
)
from datapipe_ml.training.train_config_id import build_train_config_id, short_path_label


def test_resolve_yolo_model_label_keeps_preset_names():
    assert resolve_yolo_model_label("yolo11n-pose.pt") == "yolo11n-pose"


def test_short_path_label_uses_pathy_for_cloud_paths():
    assert short_path_label("gs://bucket/runs/exp/weights/best.pt") == "best"
    assert short_path_label("s3://models/yolo11n-pose.pt") == "yolo11n-pose"


@pytest.mark.torch
def test_resolve_yolo_model_label_reads_architecture_from_local_pt():
    pytest.importorskip("torch")

    checkpoint = Path("yolo11n-pose.pt")
    if not checkpoint.is_file():
        pytest.importorskip("ultralytics")
        from ultralytics import YOLO

        YOLO("yolo11n-pose.pt")

    assert resolve_yolo_model_label(str(checkpoint.resolve())) == "yolo11n-pose"


def test_resolve_yolo_model_label_raises_when_checkpoint_metadata_unavailable():
    with pytest.raises(ValueError, match="Cannot resolve YOLO model architecture"):
        resolve_yolo_model_label("/tmp/missing/custom_pretrained.pt")


def test_resolve_yolo_model_label_raises_for_unreadable_cloud_checkpoint():
    with pytest.raises(ValueError, match="Cannot resolve YOLO model architecture"):
        resolve_yolo_model_label("gs://bucket/missing/custom_pretrained.pt")


def test_resolve_yolo_model_label_raises_for_empty_params():
    with pytest.raises(ValueError, match="no model, cfg, or initial_weights_path"):
        resolve_yolo_model_label_from_params({"weights": "", "cfg": ""}, model_key="weights")


@pytest.mark.torch
def test_yolov8_detection_train_config_uses_checkpoint_architecture_for_local_model_path():
    from dataclasses import asdict

    from datapipe_ml.frameworks.yolo.yolov8.runner import YoloV8_TrainingConfig
    from datapipe_ml.tasks.detection.train.yolov8 import get_yolov8_detection_train_configs

    pytest.importorskip("torch")

    checkpoint = Path("yolo11n-pose.pt")
    if not checkpoint.is_file():
        pytest.importorskip("ultralytics")
        from ultralytics import YOLO

        YOLO("yolo11n-pose.pt")

    config = YoloV8_TrainingConfig(
        model=str(checkpoint.resolve()),
        imgsz=640,
        batch=16,
        epochs=25,
    )
    df = next(get_yolov8_detection_train_configs([config]))
    config_id = df.iloc[0]["detection_train_config_id"]
    params = df.iloc[0]["detection_train_config__params"]

    assert config_id.startswith("yolo11n-pose-640-batch16-epochs25-cfg")
    assert config_id == build_train_config_id(params, summary=build_yolo_train_config_summary(params))
    assert asdict(config) == params
