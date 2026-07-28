from __future__ import annotations

from datetime import datetime, timezone

import pytest

from tests.utils import assert_columns_present, assert_no_nulls


def _require_datapipe_runtime():
    pytest.importorskip("tqdm")
    pytest.importorskip("datapipe")


def test_freeze_created_at_to_ts_treats_naive_as_utc():
    _require_datapipe_runtime()
    from datapipe_ml.core.datapipe import freeze_created_at_to_ts

    naive = datetime(2026, 7, 8, 21, 47, 12, 345678)
    expected = naive.replace(tzinfo=timezone.utc).timestamp()
    assert freeze_created_at_to_ts(naive) == expected
    if datetime.now().astimezone().utcoffset().total_seconds() != 0:
        assert freeze_created_at_to_ts(naive) != naive.timestamp()


def test_freeze_created_at_to_ts_keeps_aware_values():
    _require_datapipe_runtime()
    from datapipe_ml.core.datapipe import freeze_created_at_to_ts

    aware = datetime(2026, 7, 8, 21, 47, 12, 345678, tzinfo=timezone.utc)
    assert freeze_created_at_to_ts(aware) == aware.timestamp()


def test_freeze_now_utc_has_microsecond_precision():
    _require_datapipe_runtime()
    from datapipe_ml.core.datapipe import freeze_now_utc

    ts = freeze_now_utc()
    assert isinstance(ts, datetime)
    assert ts.tzinfo is None


def _store_base_inputs(ds, catalog, smoke_dataset):
    catalog.get_datatable(ds, "image").store_chunk(smoke_dataset.image)
    catalog.get_datatable(ds, "image__ground_truth").store_chunk(smoke_dataset.image_ground_truth)
    catalog.get_datatable(ds, "image__ground_truth_for_classification").store_chunk(
        smoke_dataset.image_ground_truth_for_classification
    )
    catalog.get_datatable(ds, "image__ground_truth_for_keypoints").store_chunk(
        smoke_dataset.image_ground_truth_for_keypoints
    )
    catalog.get_datatable(ds, "subset__has__image").store_chunk(smoke_dataset.subset_has_image)


def _run_single_step(ds, catalog, step):
    from datapipe.compute import Pipeline, build_compute, run_steps

    steps = build_compute(ds, catalog, Pipeline([step]))
    run_steps(ds, steps)


def _assert_frozen_dataset(ds, model_type: str, frozen_dataset_table: str, has_gt_table: str, image_count: int):
    df_frozen = ds.get_table(frozen_dataset_table).get_data()
    df_has_gt = ds.get_table(has_gt_table).get_data()

    id_col = f"{model_type}_frozen_dataset_id"
    assert len(df_frozen) == 1
    assert len(df_has_gt) == image_count
    assert_columns_present(
        df_frozen,
        [
            id_col,
            f"{model_type}_frozen_dataset__created_at",
            f"{model_type}_frozen_dataset__folder_filepath",
            f"{model_type}_frozen_dataset__images_count",
            f"{model_type}_frozen_dataset__train_images_count",
            f"{model_type}_frozen_dataset__val_images_count",
            f"{model_type}_frozen_dataset__class_names",
        ],
    )
    assert_no_nulls(df_frozen, [id_col, f"{model_type}_frozen_dataset__folder_filepath"])
    assert set(df_has_gt["subset_id"]) == {"train", "val"}
    assert df_frozen[f"{model_type}_frozen_dataset__images_count"].iloc[0] == image_count


def test_detection_freeze_dataset(base_datastore, base_catalog_factory, smoke_dataset, datapipe_dir):
    _require_datapipe_runtime()
    from datapipe_ml.tasks.detection.freeze import DetectionFreezeDataset

    catalog = base_catalog_factory()
    _store_base_inputs(base_datastore, catalog, smoke_dataset)

    _run_single_step(
        base_datastore,
        catalog,
        DetectionFreezeDataset(
            input__image="image",
            input__image__ground_truth="image__ground_truth",
            input__subset__has__image="subset__has__image",
            output__detection_frozen_dataset="detection_frozen_dataset",
            output__detection_frozen_dataset__has__image_gt="detection_frozen_dataset__has__image_gt",
            working_dir=str(datapipe_dir),
            primary_keys=["image_id"],
            min_delta=1,
            min_within_time="0s",
            create_table=True,
            bbox_id__name=None,
        ),
    )

    _assert_frozen_dataset(
        base_datastore,
        "detection",
        "detection_frozen_dataset",
        "detection_frozen_dataset__has__image_gt",
        len(smoke_dataset.image),
    )


def test_segmentation_freeze_dataset(base_datastore, base_catalog_factory, smoke_dataset, datapipe_dir):
    _require_datapipe_runtime()
    from datapipe_ml.tasks.segmentation.freeze import SegmentationFreezeDataset

    catalog = base_catalog_factory()
    _store_base_inputs(base_datastore, catalog, smoke_dataset)

    _run_single_step(
        base_datastore,
        catalog,
        SegmentationFreezeDataset(
            input__image="image",
            input__image__ground_truth="image__ground_truth",
            input__subset__has__image="subset__has__image",
            output__segmentation_frozen_dataset="segmentation_frozen_dataset",
            output__segmentation_frozen_dataset__has__image_gt="segmentation_frozen_dataset__has__image_gt",
            working_dir=str(datapipe_dir),
            primary_keys=["image_id"],
            min_delta=1,
            min_within_time="0s",
            create_table=True,
            bbox_id__name=None,
        ),
    )

    _assert_frozen_dataset(
        base_datastore,
        "segmentation",
        "segmentation_frozen_dataset",
        "segmentation_frozen_dataset__has__image_gt",
        len(smoke_dataset.image),
    )
    assert "masks" in base_datastore.get_table("segmentation_frozen_dataset__has__image_gt").get_data().columns


def test_keypoints_freeze_dataset(base_datastore, base_catalog_factory, smoke_dataset, datapipe_dir):
    _require_datapipe_runtime()
    from datapipe_ml.tasks.keypoints.freeze import KeypointsFreezeDataset

    catalog = base_catalog_factory()
    _store_base_inputs(base_datastore, catalog, smoke_dataset)

    _run_single_step(
        base_datastore,
        catalog,
        KeypointsFreezeDataset(
            input__image="image",
            input__image__ground_truth="image__ground_truth_for_keypoints",
            input__subset__has__image="subset__has__image",
            output__keypoints_frozen_dataset="keypoints_frozen_dataset",
            output__keypoints_frozen_dataset__has__image_gt="keypoints_frozen_dataset__has__image_gt",
            working_dir=str(datapipe_dir),
            primary_keys=["image_id"],
            min_delta=1,
            min_within_time="0s",
            create_table=True,
            bbox_id__name=None,
        ),
    )

    _assert_frozen_dataset(
        base_datastore,
        "keypoints",
        "keypoints_frozen_dataset",
        "keypoints_frozen_dataset__has__image_gt",
        len(smoke_dataset.image),
    )
    df = base_datastore.get_table("keypoints_frozen_dataset__has__image_gt").get_data()
    assert {"keypoints", "keypoints_visibility", "flip_idx"}.issubset(df.columns)


def test_classification_freeze_dataset(base_datastore, base_catalog_factory, smoke_dataset, datapipe_dir):
    _require_datapipe_runtime()
    from datapipe_ml.tasks.classification.freeze import ClassificationFreezeDataset

    catalog = base_catalog_factory()
    _store_base_inputs(base_datastore, catalog, smoke_dataset)

    _run_single_step(
        base_datastore,
        catalog,
        ClassificationFreezeDataset(
            input__image="image",
            input__image__ground_truth="image__ground_truth_for_classification",
            input__subset__has__image="subset__has__image",
            output__classification_frozen_dataset="classification_frozen_dataset",
            output__classification_frozen_dataset__has__image_gt="classification_frozen_dataset__has__image_gt",
            working_dir=str(datapipe_dir),
            primary_keys=["image_id"],
            min_delta=1,
            min_within_time="0s",
            create_table=True,
        ),
    )

    _assert_frozen_dataset(
        base_datastore,
        "classification",
        "classification_frozen_dataset",
        "classification_frozen_dataset__has__image_gt",
        len(smoke_dataset.image),
    )
    assert_no_nulls(base_datastore.get_table("classification_frozen_dataset__has__image_gt").get_data(), ["label"])


def test_freeze_skips_repeat_when_ground_truth_unchanged(base_datastore, base_catalog_factory, smoke_dataset, datapipe_dir):
    _require_datapipe_runtime()
    from datapipe_ml.tasks.detection.freeze import DetectionFreezeDataset

    catalog = base_catalog_factory()
    _store_base_inputs(base_datastore, catalog, smoke_dataset)

    step = DetectionFreezeDataset(
        input__image="image",
        input__image__ground_truth="image__ground_truth",
        input__subset__has__image="subset__has__image",
        output__detection_frozen_dataset="detection_frozen_dataset",
        output__detection_frozen_dataset__has__image_gt="detection_frozen_dataset__has__image_gt",
        working_dir=str(datapipe_dir),
        primary_keys=["image_id"],
        min_delta=1,
        min_within_time="0s",
        create_table=True,
        bbox_id__name=None,
    )

    _run_single_step(base_datastore, catalog, step)
    frozen_after_first = len(base_datastore.get_table("detection_frozen_dataset").get_data())
    assert frozen_after_first == 1

    _run_single_step(base_datastore, catalog, step)

    assert len(base_datastore.get_table("detection_frozen_dataset").get_data()) == frozen_after_first
