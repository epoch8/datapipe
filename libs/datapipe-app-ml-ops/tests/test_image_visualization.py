from __future__ import annotations

import pytest


pytest.importorskip("cv_pipeliner")

from datapipe_app_ml_ops.viz.image_visualization import bboxes_data_from_record


def test_bboxes_data_from_record_includes_keypoints_and_masks():
    record = {
        "bboxes": [[10, 20, 100, 120], [30, 40, 80, 90]],
        "labels": ["cat", "dog"],
        "keypoints": [[[1, 2], [3, 4]], [[5, 6], [7, 8], [9, 10]]],
        "masks": [[[[1, 1], [2, 2]]], []],
        "prediction__detection_scores": [0.9, 0.8],
        "prediction__keypoints_scores": [[0.95, 0.85], [0.7, 0.6, 0.5]],
        "keypoints_visibility": [[0, 2], [1, 2, 2]],
    }

    bboxes_data = bboxes_data_from_record(record)

    assert len(bboxes_data) == 2
    assert bboxes_data[0].keypoints.tolist() == [[1, 2], [3, 4]]
    assert bboxes_data[1].keypoints.tolist() == [[5, 6], [7, 8], [9, 10]]
    assert len(bboxes_data[0].mask) == 1
    assert bboxes_data[1].mask == []
    assert bboxes_data[0].detection_score == 0.9
    assert bboxes_data[0].keypoints_scores == [0.95, 0.85]
    assert [int(v) for v in bboxes_data[0].keypoints_visibility] == [0, 2]
    assert [int(v) for v in bboxes_data[1].keypoints_visibility] == [1, 2, 2]
