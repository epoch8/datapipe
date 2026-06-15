from __future__ import annotations

import pytest


def _assert_yolo_resume_params(algo) -> None:  # noqa: ANN001
    params = {"epochs": 2, "save_period": -1}

    updated = algo.apply_resume_checkpoint(None, params, "checkpoint.pt")

    assert updated["initial_weights_path"] == "checkpoint.pt"
    assert updated["resume"] is True
    assert updated["exist_ok"] is True
    assert updated["save_period"] == 1
    assert "initial_weights_path" not in params


@pytest.mark.torch
def test_yolov8_detection_resume_adapter_sets_typed_params() -> None:
    pytest.importorskip("ultralytics")
    from datapipe_ml.tasks.detection.train.yolov8 import YoloV8DetectionAlgo

    _assert_yolo_resume_params(YoloV8DetectionAlgo())


@pytest.mark.torch
def test_yolov5_detection_resume_adapter_sets_typed_params() -> None:
    pytest.importorskip("yolov5")
    from datapipe_ml.tasks.detection.train.yolov5 import YoloV5DetectionAlgo

    params = {"epochs": 2, "save_period": -1}
    updated = YoloV5DetectionAlgo().apply_resume_checkpoint(None, params, "checkpoint.pt")

    assert updated["resume"] == "checkpoint.pt"
    assert "initial_weights_path" not in updated
    assert updated["exist_ok"] is True
    assert updated["save_period"] == 1
    assert "initial_weights_path" not in params


@pytest.mark.torch
def test_yolov8_segmentation_resume_adapter_sets_typed_params() -> None:
    pytest.importorskip("ultralytics")
    from datapipe_ml.tasks.segmentation.train.yolov8 import YoloV8SegmentationAlgo

    _assert_yolo_resume_params(YoloV8SegmentationAlgo())


@pytest.mark.torch
def test_yolov8_keypoints_resume_adapter_sets_typed_params() -> None:
    pytest.importorskip("ultralytics")
    from datapipe_ml.tasks.keypoints.train.yolov8 import YoloV8KeypointsAlgo

    _assert_yolo_resume_params(YoloV8KeypointsAlgo())


@pytest.mark.tensorflow
def test_tensorflow_resume_adapter_sets_explicit_checkpoint_param() -> None:
    pytest.importorskip("tensorflow")
    from datapipe_ml.tasks.classification.train.tensorflow import TFClassificationAlgo

    params = {"epochs": 2}
    updated = TFClassificationAlgo().apply_resume_checkpoint(None, params, "model.keras", checkpoint_epoch=7)

    assert updated["__resume_checkpoint_filepath"] == "model.keras"
    assert "__resume_checkpoint_filepath" not in params
    assert "__resume_checkpoint_epoch" not in params


@pytest.mark.tensorflow
def test_tensorflow_resume_adapter_omits_epoch_when_not_provided() -> None:
    pytest.importorskip("tensorflow")
    from datapipe_ml.tasks.classification.train.tensorflow import TFClassificationAlgo

    params = {"epochs": 2}
    updated = TFClassificationAlgo().apply_resume_checkpoint(None, params, "model.keras")

    assert updated["__resume_checkpoint_filepath"] == "model.keras"
    assert "__resume_checkpoint_epoch" not in updated
