from __future__ import annotations

import pandas as pd
import pytest

pytestmark = [pytest.mark.e2e_examples, pytest.mark.e2e_template]


def _pipeline_labels(pipeline):
    return {label for step in pipeline.steps for label in (step.labels or [])}


def test_detection_template_builds_expected_pipeline():
    from helpers import load_template_module, require_postgres

    require_postgres()
    detection_pipeline = load_template_module("detection", "app").pipeline

    assert len(detection_pipeline.steps) >= 15
    assert {
        ("stage", "annotation"),
        ("stage", "ls-sync"),
        ("stage", "train"),
        ("stage", "train-prepare"),
        ("stage", "inference"),
        ("stage", "count-metrics"),
        ("stage", "fiftyone"),
    }.issubset(_pipeline_labels(detection_pipeline))


def test_keypoints_template_builds_expected_pipeline():
    from helpers import load_template_module, require_postgres

    require_postgres()
    keypoints_pipeline = load_template_module("keypoints", "app").pipeline

    assert len(keypoints_pipeline.steps) >= 15
    assert {
        ("stage", "annotation"),
        ("stage", "ls-sync"),
        ("stage", "train"),
        ("stage", "train-prepare"),
        ("stage", "inference"),
        ("stage", "count-metrics"),
        ("stage", "fiftyone"),
    }.issubset(_pipeline_labels(keypoints_pipeline))


def test_detection_template_catalog_has_expected_tables():
    from helpers import load_template_module, require_postgres

    require_postgres()
    catalog = load_template_module("detection", "data").catalog
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


def test_keypoints_template_catalog_has_expected_tables():
    from helpers import load_template_module, require_postgres

    require_postgres()
    catalog = load_template_module("keypoints", "data").catalog
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
    from helpers import load_template_module

    split_detection_train_val = load_template_module("detection", "steps").split_df_train_val
    split_keypoints_train_val = load_template_module("keypoints", "steps").split_df_train_val

    df = pd.DataFrame({"image_name": [f"image-{idx}" for idx in range(4)]})
    subset_df = pd.DataFrame(columns=["image_name", "subset_id"])
    split_kwargs = dict(
        primary_keys=["image_name"],
        val_perc=0.25,
        random_seed=42,
    )

    detection_split = split_detection_train_val(df, subset_df, **split_kwargs)
    keypoints_split = split_keypoints_train_val(df, subset_df, **split_kwargs)

    assert detection_split.to_dict("records") == keypoints_split.to_dict("records")
    assert detection_split["subset_id"].value_counts().to_dict() == {"train": 3, "val": 1}


def test_template_split_helpers_preserve_existing_subset():
    from helpers import load_template_module

    split_train_val = load_template_module("detection", "steps").split_df_train_val

    df = pd.DataFrame(
        {
            "image_name": ["image-0", "image-1", "image-2", "image-3"],
            "bboxes": [[], [], [], []],
            "labels": [[], [], [], []],
        }
    )
    subset_df = pd.DataFrame(
        {
            "image_name": ["image-0", "image-1"],
            "subset_id": ["train", "val"],
        }
    )

    result = split_train_val(
        df,
        subset_df,
        primary_keys=["image_name"],
        val_perc=0.25,
        random_seed=42,
    )

    assert result.set_index("image_name").loc["image-0", "subset_id"] == "train"
    assert result.set_index("image_name").loc["image-1", "subset_id"] == "val"
    assert result["subset_id"].value_counts().to_dict() == {"train": 3, "val": 1}
