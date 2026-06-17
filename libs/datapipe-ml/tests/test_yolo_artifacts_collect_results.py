from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

import pytest

from datapipe_ml.frameworks.yolo.artifacts import yolo_collect_results_generic


@dataclass
class _CollectResultStub:
    detection_model_id: str
    class_names: List[str]
    epoch: int
    model_path: Optional[str]
    metrics_mAP_0_5: float
    metrics_mAP_0_5_to_0_95: float
    f1_curve_image: Optional[object] = None
    best_threshold: Optional[float] = None
    objects_count: Optional[int] = None


def test_yolo_collect_results_resolves_best_and_last_without_epoch_files(tmp_path) -> None:
    exp_dir = tmp_path / "exp"
    weights_dir = exp_dir / "weights"
    weights_dir.mkdir(parents=True)
    (weights_dir / "best.pt").write_bytes(b"best")
    (weights_dir / "last.pt").write_bytes(b"last")

    (exp_dir / "results.csv").write_text(
        "\n".join(
            [
                "epoch,metrics/mAP50(B),metrics/mAP50-95(B)",
                "1,0.10,0.05",
                "2,0.80,0.70",
                "3,0.60,0.50",
            ]
        )
    )

    results = yolo_collect_results_generic(
        exp_folder=str(exp_dir),
        result_cls=_CollectResultStub,
        id_field_name="detection_model_id",
        id_field_value="run-1",
        class_names=["cat"],
        objects_count=1,
        f1_image_field_name="f1_curve_image",
        f1_image=None,
        best_threshold=0.45,
        rename_map={
            "metrics/mAP50(B)": "metrics_mAP_0_5",
            "metrics/mAP50-95(B)": "metrics_mAP_0_5_to_0_95",
        },
        best_metric_col="metrics_mAP_0_5_to_0_95",
        weights_subdir="weights",
    )

    by_epoch = {result.epoch: result for result in results}
    assert set(by_epoch) == {2, 3}
    assert by_epoch[2].model_path.endswith("/weights/best.pt")
    assert by_epoch[3].model_path.endswith("/weights/last.pt")


def test_yolo_collect_results_raises_when_best_epoch_has_no_weight(tmp_path) -> None:
    exp_dir = tmp_path / "exp"
    weights_dir = exp_dir / "weights"
    weights_dir.mkdir(parents=True)
    (weights_dir / "last.pt").write_bytes(b"last")

    (exp_dir / "results.csv").write_text(
        "\n".join(
            [
                "epoch,metrics/mAP50(B),metrics/mAP50-95(B)",
                "1,0.10,0.05",
                "2,0.80,0.70",
                "3,0.60,0.50",
            ]
        )
    )

    with pytest.raises(FileNotFoundError, match="best epoch 2"):
        yolo_collect_results_generic(
            exp_folder=str(exp_dir),
            result_cls=_CollectResultStub,
            id_field_name="detection_model_id",
            id_field_value="run-1",
            class_names=["cat"],
            objects_count=1,
            f1_image_field_name="f1_curve_image",
            f1_image=None,
            best_threshold=0.45,
            rename_map={
                "metrics/mAP50(B)": "metrics_mAP_0_5",
                "metrics/mAP50-95(B)": "metrics_mAP_0_5_to_0_95",
            },
            best_metric_col="metrics_mAP_0_5_to_0_95",
            weights_subdir="weights",
        )
