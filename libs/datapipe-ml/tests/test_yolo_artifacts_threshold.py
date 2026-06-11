from __future__ import annotations

import numpy as np

from datapipe_ml.frameworks.yolo.artifacts import yolo_best_threshold_from_ultralytics_metrics


class _MetricsStub:
    def __init__(self, curves, curves_results):
        self.curves = curves
        self.curves_results = curves_results


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

    assert 0.3 <= threshold <= 1.0
