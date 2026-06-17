import pandas as pd
import pytest

from datapipe_ml.tasks.detection.inference import min_prediction_threshold_from_class_thresholds
from datapipe_ml.training.accelerator import cpu_training_allowed, gpu_training_explicitly_disabled


def test_min_prediction_threshold_from_class_thresholds() -> None:
    df = pd.DataFrame(
        {
            "class_name_to_threshold": [
                {"cat": 0.5, "dog": 0.2},
                {"bird": 0.35},
            ]
        }
    )
    assert min_prediction_threshold_from_class_thresholds(df, "class_name_to_threshold") == pytest.approx(0.2)


def test_cpu_training_allowed_for_explicit_device() -> None:
    assert cpu_training_allowed({"device": "cpu"}) is True


def test_gpu_training_explicitly_disabled_via_cuda_visible_devices(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CUDA_VISIBLE_DEVICES", "")
    assert gpu_training_explicitly_disabled() is True
    assert cpu_training_allowed({}) is True


def test_gpu_training_not_disabled_when_cuda_env_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("CUDA_VISIBLE_DEVICES", raising=False)
    monkeypatch.delenv("NVIDIA_VISIBLE_DEVICES", raising=False)
    assert gpu_training_explicitly_disabled() is False
