from __future__ import annotations

import pandas as pd
import pytest

pytestmark = [pytest.mark.e2e_examples, pytest.mark.e2e_template]


def _pipeline_labels(pipeline):
    return {label for step in pipeline.steps for label in (step.labels or [])}


def test_detection_template_builds_expected_pipeline(tmp_path):
    from examples.e2e_template.common import make_sqlite_settings
    from examples.e2e_template.image_detection.app import build_pipeline as build_detection_pipeline
    from examples.e2e_template.image_detection.data import build_catalog as build_detection_catalog

    settings = make_sqlite_settings(tmp_path, pipeline_name="detection")
    pipeline = build_detection_pipeline(settings, include_training=True)
    catalog = build_detection_catalog(settings)

    assert len(pipeline.steps) >= 15
    assert {
        ("stage", "annotation"),
        ("stage", "ls-sync"),
        ("stage", "train"),
        ("stage", "train-prepare"),
        ("stage", "inference"),
        ("stage", "count-metrics"),
        ("stage", "fiftyone"),
    }.issubset(_pipeline_labels(pipeline))
    assert {
        "s3_images",
        "detection_model",
        "detection_predictions",
        "images_with_predictions",
        "image__ground_truth",
        "image__subset",
        "local_images",
        "fiftyone_predictions",
        "fiftyone_annotations",
    }.issubset(catalog.catalog)


def test_keypoints_template_builds_expected_pipeline(tmp_path):
    from examples.e2e_template.common import make_sqlite_settings
    from examples.e2e_template.image_keypoints.app import build_pipeline as build_keypoints_pipeline
    from examples.e2e_template.image_keypoints.data import build_catalog as build_keypoints_catalog

    settings = make_sqlite_settings(tmp_path, pipeline_name="keypoints")
    pipeline = build_keypoints_pipeline(settings, include_training=True)
    catalog = build_keypoints_catalog(settings)

    assert len(pipeline.steps) >= 15
    assert {
        ("stage", "annotation"),
        ("stage", "ls-sync"),
        ("stage", "train"),
        ("stage", "train-prepare"),
        ("stage", "inference"),
        ("stage", "count-metrics"),
        ("stage", "fiftyone"),
    }.issubset(_pipeline_labels(pipeline))
    assert {
        "s3_images",
        "keypoints_model",
        "keypoints_predictions",
        "images_with_predictions",
        "image__ground_truth",
        "image__subset",
        "local_images",
        "fiftyone_predictions",
        "fiftyone_annotations",
    }.issubset(catalog.catalog)


def test_template_split_helpers_are_deterministic():
    from examples.e2e_template.image_detection.steps import split_df_train_val as split_detection_train_val
    from examples.e2e_template.image_keypoints.steps import split_df_train_val as split_keypoints_train_val

    df = pd.DataFrame({"image_name": [f"image-{idx}" for idx in range(4)]})

    detection_split = split_detection_train_val(df, val_perc=0.25)
    keypoints_split = split_keypoints_train_val(df, val_perc=0.25)

    assert detection_split.to_dict("records") == keypoints_split.to_dict("records")
    assert detection_split["subset_id"].tolist() == ["train", "train", "train", "train"]

