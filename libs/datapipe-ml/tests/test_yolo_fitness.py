from __future__ import annotations

from datapipe_ml.frameworks.yolo.training import yolo_fitness


def test_yolo_fitness_prefers_map5095() -> None:
    low_map5095 = yolo_fitness(0.9, 0.1)
    high_map5095 = yolo_fitness(0.1, 0.9)

    assert high_map5095 > low_map5095
