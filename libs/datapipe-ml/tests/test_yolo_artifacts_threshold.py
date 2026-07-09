from __future__ import annotations

import numpy as np

from datapipe_ml.frameworks.yolo.artifacts import (
    YOLO_MIN_SCORE_THRESHOLD,
    yolo_best_threshold_from_ultralytics_metrics,
    yolo_smooth_f1_curve,
)


class _MetricsStub:
    def __init__(self, curves, curves_results):
        self.curves = curves
        self.curves_results = curves_results


def test_yolo_smooth_f1_curve_returns_same_length_as_input() -> None:
    y = np.array([0.1, 0.5, 0.9, 0.4, 0.2], dtype=float)
    smoothed = yolo_smooth_f1_curve(y, 0.1)

    assert smoothed.shape == y.shape


def test_yolo_best_threshold_accepts_ultralytics_curve_with_extra_values():
    x = np.linspace(0, 1, 5)
    y = np.array([[0.1, 0.4, 0.9, 0.6, 0.2], [0.2, 0.3, 0.8, 0.5, 0.1]])
    metrics = _MetricsStub(
        curves=["Precision-Recall(B)", "F1-Confidence(B)"],
        curves_results=[
            [x, y, np.zeros(5)],
            [x, y],
        ],
    )

    threshold = yolo_best_threshold_from_ultralytics_metrics(metrics, "F1-Confidence(B)")

    assert YOLO_MIN_SCORE_THRESHOLD <= threshold <= 1.0


def test_yolo_best_threshold_floor_is_001_when_f1_peak_at_zero() -> None:
    from datapipe_ml.frameworks.yolo.artifacts import yolo_best_threshold_from_curve

    x = np.array([0.0, 0.25, 0.5, 0.75, 1.0])
    y = np.array([0.9, 0.5, 0.3, 0.2, 0.1])

    assert yolo_best_threshold_from_curve(x, y) == YOLO_MIN_SCORE_THRESHOLD
