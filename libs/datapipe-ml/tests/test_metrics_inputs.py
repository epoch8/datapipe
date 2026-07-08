from __future__ import annotations

import pandas as pd

from datapipe_ml.metrics.inputs import ground_truth_convert_keys, prediction_convert_keys, wrap_ground_truth_inputs


def test_wrap_ground_truth_inputs_merges_tables_before_metrics_func():
    def count_metrics(df__image__ground_truth, df__subset):
        return df__image__ground_truth.merge(df__subset, on="image_name")

    wrapped = wrap_ground_truth_inputs(
        count_metrics,
        n_ground_truth_inputs=2,
        primary_keys=["image_name"],
    )
    gt = pd.DataFrame({"image_name": ["img-1"], "bboxes": [[[]]], "labels": [[[]]]})
    tags = pd.DataFrame({"image_name": ["img-1"], "tag_id": ["batch_night"]})
    subset = pd.DataFrame({"image_name": ["img-1"], "subset_id": ["val"]})

    result = wrapped(gt, tags, subset)

    assert result.to_dict("records") == [
        {"image_name": "img-1", "bboxes": [[]], "labels": [[]], "tag_id": "batch_night", "subset_id": "val"},
    ]


def test_ground_truth_convert_keys_includes_model_keys_present_in_gt():
    df = pd.DataFrame(
        {
            "image_name": ["img-1"],
            "subset_id": ["val"],
            "tag_id": ["batch_night"],
            "bboxes": [[[]]],
        }
    )

    keys = ground_truth_convert_keys(
        primary_keys=["image_name"],
        model_primary_keys=["detection_model_id", "tag_id"],
        df__ground_truth=df,
    )

    assert keys == ["image_name", "subset_id", "tag_id"]


def test_prediction_convert_keys_skips_model_keys_missing_in_prediction():
    df = pd.DataFrame(
        {
            "image_name": ["img-1"],
            "subset_id": ["val"],
            "detection_model_id": ["m1"],
            "bboxes": [[[]]],
        }
    )

    keys = prediction_convert_keys(
        primary_keys=["image_name"],
        model_primary_keys=["detection_model_id", "tag_id"],
        df__prediction=df,
    )

    assert keys == ["image_name", "detection_model_id", "subset_id"]
