from __future__ import annotations

import pytest

from datapipe_ml.training.config_codec import (
    TrainConfigValidationError,
    get_train_config_codec,
    register_train_config_codec,
)


class _FakeCodec:
    config_type = "fake_codec_for_tests"

    def normalize(self, params):
        return dict(params)

    def validate(self, params):
        return dict(params)

    def json_schema(self):
        return {"type": "object"}

    def summarize(self, params):
        return {"display": "fake"}


def test_register_and_get_codec():
    codec = _FakeCodec()
    register_train_config_codec(codec)
    assert get_train_config_codec("fake_codec_for_tests") is codec


def test_register_duplicate_codec_raises():
    class _Dup:
        config_type = "dup_codec_for_tests"

        def normalize(self, params):
            return dict(params)

        def validate(self, params):
            return dict(params)

        def json_schema(self):
            return {}

        def summarize(self, params):
            return {}

    register_train_config_codec(_Dup())
    with pytest.raises(ValueError):
        register_train_config_codec(_Dup())


def test_get_unknown_codec_raises():
    with pytest.raises(ValueError, match="Unknown train config type"):
        get_train_config_codec("does_not_exist")


def test_validation_error_carries_structured_errors():
    err = TrainConfigValidationError("bad", errors=[{"field": "imgsz", "message": "too small"}])
    assert isinstance(err, ValueError)
    assert err.errors == [{"field": "imgsz", "message": "too small"}]


def _yolo_codec(config_type: str = "yolov8_detection"):
    pytest.importorskip("ultralytics")
    import datapipe_ml.frameworks.yolo.train_config_codec  # noqa: F401

    return get_train_config_codec(config_type)


def test_yolov8_codec_is_registered():
    codec = _yolo_codec()
    assert codec.config_type == "yolov8_detection"


def test_yolov8_task_codecs_use_dataclass_default_models():
    """Defaults come from task dataclasses, not a hardcoded map in the codec."""
    pytest.importorskip("ultralytics")
    from datapipe_ml.frameworks.yolo.yolov8.runner import (
        YoloV8DetectionTrainingConfig,
        YoloV8KeypointsTrainingConfig,
        YoloV8SegmentationTrainingConfig,
    )

    expected = {
        "yolov8_detection": YoloV8DetectionTrainingConfig,
        "yolov8_segmentation": YoloV8SegmentationTrainingConfig,
        "yolov8_keypoints": YoloV8KeypointsTrainingConfig,
    }
    for config_type, config_cls in expected.items():
        codec = _yolo_codec(config_type)
        model = config_cls().model
        assert codec.config_cls is config_cls
        assert codec.json_schema()["properties"]["model"]["default"] == model
        assert codec.default_params()["model"] == model
        assert codec.validate({})["model"] == model


def test_yolov8_codec_json_schema_has_min_fields():
    codec = _yolo_codec()
    schema = codec.json_schema()
    props = schema["properties"]
    for field in ("model", "imgsz", "batch", "epochs", "optimizer", "lr0", "weight_decay", "seed", "patience"):
        assert field in props
    assert schema["type"] == "object"
    assert props["imgsz"]["ui_group"] == "model"
    assert "auto" in props["optimizer"]["enum"]
    # Defaults come from the task dataclass so the UI can prefill every field.
    assert props["epochs"]["default"] == 300
    assert props["optimizer"]["default"] == "auto"
    assert props["mosaic"]["default"] == 0.0
    # Infrastructure / pipeline-owned fields stay out of the form schema.
    for excluded in ("data", "project", "tmp_folder", "resume", "name"):
        assert excluded not in props
    # Every editable dataclass field is present (minus the excluded set).
    assert "degrees" in props
    assert "device" in props
    assert len(props) >= 50


def test_yolov8_codec_default_params_match_schema_defaults():
    codec = _yolo_codec()
    defaults = codec.default_params()
    assert defaults["model"] == codec.config_cls().model
    assert defaults["batch"] == 16
    assert defaults["lr0"] == 0.01
    assert "data" not in defaults
    assert defaults["device"] is None


def test_yolov8_codec_validate_rejects_unknown_fields():
    codec = _yolo_codec()
    with pytest.raises(TrainConfigValidationError) as exc_info:
        codec.validate({"not_a_field": 1})
    assert exc_info.value.errors
    assert exc_info.value.errors[0]["field"] == "not_a_field"


def _tf_codec():
    pytest.importorskip("tensorflow")
    import datapipe_ml.frameworks.tensorflow.train_config_codec  # noqa: F401

    return get_train_config_codec("tf_classification")


def test_tf_classification_codec_is_registered():
    codec = _tf_codec()
    assert codec.config_type == "tf_classification"
    schema = codec.json_schema()
    for field in ("arch", "image_size", "batch_size", "epochs", "init_lr"):
        assert field in schema["properties"]
    assert schema["properties"]["arch"]["default"] == "mobilenet"
    assert schema["properties"]["image_size"]["default"] == (224, 224)
    assert schema["properties"]["batch_size"]["default"] == 128
    assert schema["properties"]["epochs"]["default"] == 100
    assert schema["properties"]["seed"]["default"] == 0
    assert schema["properties"]["init_lr"]["default"] == 0.001
    assert codec.default_params()["arch"] == "mobilenet"
    normalized = codec.validate({})
    assert normalized["arch"] == "mobilenet"
    assert list(normalized["image_size"]) == [224, 224]
    summary = codec.summarize(
        {"arch": "tiny_cnn", "image_size": [128, 128], "batch_size": 8, "epochs": 20}
    )
    assert "tiny_cnn" in summary["display"]


def test_yolov8_codec_validate_applies_defaults():
    codec = _yolo_codec()
    normalized = codec.validate({"model": "yolov8s.pt", "imgsz": 640, "batch": 8, "epochs": 5})
    # defaults from the dataclass are present in the normalized params
    assert normalized["optimizer"] == "auto"
    assert normalized["model"] == "yolov8s.pt"
    assert normalized["imgsz"] == 640


def test_yolov8_codec_summarize():
    codec = _yolo_codec()
    summary = codec.summarize({"model": "yolov8m.pt", "imgsz": 1280, "batch": 4, "epochs": 50, "optimizer": "auto"})
    assert summary["model"] == "yolov8m.pt"
    assert summary["image_size"] == 1280
    assert summary["batch_size"] == 4
    assert summary["epochs"] == 50
    assert summary["optimizer"] == "auto"
    assert "1280px" in summary["display"]
    assert "50 epochs" in summary["display"]
