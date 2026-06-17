from __future__ import annotations

import pandas as pd

from tests.fixtures.smoke_data import (
    define_ground_truth_for_classification,
    generate_smoke_data,
    get_most_common_label,
)
from tests.utils import assert_columns_present, assert_no_nulls


def test_smoke_dataset_has_aligned_tables(smoke_dataset):
    assert len(smoke_dataset.image) == 6
    assert len(smoke_dataset.image_ground_truth) == 6
    assert len(smoke_dataset.subset_has_image) == 6

    image_ids = set(smoke_dataset.image["image_id"])
    assert set(smoke_dataset.image_ground_truth["image_id"]) == image_ids
    assert set(smoke_dataset.subset_has_image["image_id"]) == image_ids
    assert set(smoke_dataset.subset_has_image["subset_id"]) == {"train", "val"}

    assert_columns_present(smoke_dataset.image_ground_truth, ["bboxes", "labels", "masks"])
    assert_no_nulls(smoke_dataset.image, ["image_id", "image__image_path"])


def test_generate_smoke_data_yields_input_frames(smoke_dataset):
    generated = list(generate_smoke_data(smoke_dataset))
    assert len(generated) == 1

    df_image, df_gt, df_subset = generated[0]
    pd.testing.assert_frame_equal(df_image, smoke_dataset.image)
    pd.testing.assert_frame_equal(df_gt, smoke_dataset.image_ground_truth)
    pd.testing.assert_frame_equal(df_subset, smoke_dataset.subset_has_image)


def test_get_most_common_label_uses_lexicographic_tie_break():
    assert get_most_common_label(["dog", "cat", "dog"]) == "dog"
    assert get_most_common_label(["dog", "cat"]) == "cat"


def test_define_ground_truth_for_classification(smoke_dataset):
    df_cls = define_ground_truth_for_classification(smoke_dataset.image_ground_truth)

    assert list(df_cls.columns) == ["image_id", "label"]
    assert len(df_cls) == len(smoke_dataset.image_ground_truth)
    assert_no_nulls(df_cls, ["image_id", "label"])
    assert df_cls.loc[df_cls["image_id"] == "image_000", "label"].iloc[0] == "cat"
