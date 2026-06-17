from __future__ import annotations

import pytest


@pytest.mark.torch
def test_resolve_installed_yolov5_train_script_points_to_packaged_train_py() -> None:
    pytest.importorskip("yolov5")
    from datapipe_ml.frameworks.yolo.yolov5.runner import resolve_installed_yolov5_train_script

    train_script = resolve_installed_yolov5_train_script()

    assert train_script.name == "train.py"
    assert train_script.exists()
    assert train_script.parent.name == "yolov5"
