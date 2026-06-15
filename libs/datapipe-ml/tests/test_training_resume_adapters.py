from __future__ import annotations

from pathlib import Path

import pandas as pd
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
def test_tensorflow_launch_training_passes_resume_checkpoint(monkeypatch, tmp_path: Path) -> None:
    pytest.importorskip("tensorflow")
    from datapipe_ml.frameworks.tensorflow.classification_runner import TrainModelResult
    from datapipe_ml.tasks.classification.train.tensorflow import (
        TFClassificationAlgo,
        TensorflowClassificationPreparedData,
        TensorflowClassificationTrainContext,
    )
    from datapipe_ml.training.specs import TrainingResumeCheckpoint

    captured_args: list[tuple] = []

    class FakeLauncher:
        def launch(self, request):
            captured_args.append(request.args)
            return TrainModelResult(
                classification_model_id="cls-id",
                model_path=str(tmp_path / "model.keras"),
                class_names=["a"],
                preprocess_input_script_path=None,
            )

    monkeypatch.setattr(
        "datapipe_ml.tasks.classification.train.tensorflow.build_training_launcher",
        lambda _: FakeLauncher(),
    )

    ctx = TensorflowClassificationTrainContext(
        models_dir=str(tmp_path / "models"),
        max_within_time="1d",
        tmp_folder=str(tmp_path / "tmp"),
        model_suffix="_s",
        dt__model=None,  # type: ignore[arg-type]
        dt__link=None,  # type: ignore[arg-type]
        dt__training_status=None,  # type: ignore[arg-type]
        dt__frozen_dataset=None,  # type: ignore[arg-type]
        dt__frozen_dataset__has__image_gt=None,
        dt__train_config=None,  # type: ignore[arg-type]
        model_other_primary_keys=[],
        model_id__name="model_id",
        frozen_dataset_id__name="frozen_dataset_id",
        clean_checkpoints_after_train=False,
    )
    data = TensorflowClassificationPreparedData(
        df__frozen_dataset__has__image_gt=pd.DataFrame({"image__image_path": [str(tmp_path / "img.jpg")]})
    )
    train_params = {
        "image_size": (32, 32),
        "seed": 0,
        "batch_size": 1,
        "arch": "resnet50",
        "init_lr": 0.001,
        "reduce_lr_patience": 1,
        "reduce_lr_factor": 0.5,
        "early_stopping_patience": 1,
        "epochs": 1,
        "label_smoothing": 0.0,
        "augmentations": False,
        "augment_func_file": None,
        "class_weight": False,
    }

    TFClassificationAlgo().launch_training(
        ctx,
        pd.DataFrame([{}]),
        "model-id",
        train_params,
        data,
        resume_checkpoint=TrainingResumeCheckpoint(path="model.keras", epoch=7),
    )

    assert captured_args[0][6] == "model.keras"
    assert captured_args[0][7] == 7


@pytest.mark.tensorflow
def test_tensorflow_launch_training_omits_resume_when_not_provided(monkeypatch, tmp_path: Path) -> None:
    pytest.importorskip("tensorflow")
    from datapipe_ml.frameworks.tensorflow.classification_runner import TrainModelResult
    from datapipe_ml.tasks.classification.train.tensorflow import (
        TFClassificationAlgo,
        TensorflowClassificationPreparedData,
        TensorflowClassificationTrainContext,
    )

    captured_args: list[tuple] = []

    class FakeLauncher:
        def launch(self, request):
            captured_args.append(request.args)
            return TrainModelResult(
                classification_model_id="cls-id",
                model_path=str(tmp_path / "model.keras"),
                class_names=["a"],
                preprocess_input_script_path=None,
            )

    monkeypatch.setattr(
        "datapipe_ml.tasks.classification.train.tensorflow.build_training_launcher",
        lambda _: FakeLauncher(),
    )

    ctx = TensorflowClassificationTrainContext(
        models_dir=str(tmp_path / "models"),
        max_within_time="1d",
        tmp_folder=str(tmp_path / "tmp"),
        model_suffix="_s",
        dt__model=None,  # type: ignore[arg-type]
        dt__link=None,  # type: ignore[arg-type]
        dt__training_status=None,  # type: ignore[arg-type]
        dt__frozen_dataset=None,  # type: ignore[arg-type]
        dt__frozen_dataset__has__image_gt=None,
        dt__train_config=None,  # type: ignore[arg-type]
        model_other_primary_keys=[],
        model_id__name="model_id",
        frozen_dataset_id__name="frozen_dataset_id",
        clean_checkpoints_after_train=False,
    )
    data = TensorflowClassificationPreparedData(
        df__frozen_dataset__has__image_gt=pd.DataFrame({"image__image_path": [str(tmp_path / "img.jpg")]})
    )
    train_params = {
        "image_size": (32, 32),
        "seed": 0,
        "batch_size": 1,
        "arch": "resnet50",
        "init_lr": 0.001,
        "reduce_lr_patience": 1,
        "reduce_lr_factor": 0.5,
        "early_stopping_patience": 1,
        "epochs": 1,
        "label_smoothing": 0.0,
        "augmentations": False,
        "augment_func_file": None,
        "class_weight": False,
    }

    TFClassificationAlgo().launch_training(
        ctx,
        pd.DataFrame([{}]),
        "model-id",
        train_params,
        data,
    )

    assert captured_args[0][6] is None
    assert captured_args[0][7] is None
