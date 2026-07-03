from __future__ import annotations

import pandas as pd
import pytest
from sqlalchemy import JSON, Column, Float, Integer
from sqlalchemy.sql.sqltypes import String


def _require_metrics_runtime():
    pytest.importorskip("tqdm")
    pytest.importorskip("datapipe")
    pytest.importorskip("cv_pipeliner")


def _first_batch_transform_step(steps):
    from datapipe.step.batch_transform import BaseBatchTransformStep

    for step in steps:
        if isinstance(step, BaseBatchTransformStep):
            return step
    raise AssertionError("BaseBatchTransformStep not found")


def _assert_inner_join_inputs(step, table_names: set[str]) -> None:
    for inp in step.input_dts:
        if inp.dt.name in table_names:
            assert inp.join_type == "inner", f"{inp.dt.name} must use inner join, got {inp.join_type!r}"


def _pipeline_catalog(dbconn):
    from datapipe.compute import Catalog, Table
    from datapipe.store.database import TableStoreDB

    return Catalog(
        {
            "image_gt": Table(
                store=TableStoreDB(
                    dbconn,
                    "image_gt",
                    [
                        Column("image_id", String, primary_key=True),
                        Column("bboxes", JSON),
                        Column("labels", JSON),
                    ],
                    True,
                )
            ),
            "subset_has_image": Table(
                store=TableStoreDB(
                    dbconn,
                    "subset_has_image",
                    [
                        Column("image_id", String, primary_key=True),
                        Column("subset_id", String, primary_key=True),
                    ],
                    True,
                )
            ),
            "pipeline_prediction": Table(
                store=TableStoreDB(
                    dbconn,
                    "pipeline_prediction",
                    [
                        Column("image_id", String, primary_key=True),
                        Column("detection_model_id", String, primary_key=True),
                        Column("classification_model_id", String, primary_key=True),
                        Column("bboxes", JSON),
                        Column("labels", JSON),
                        Column("prediction__detection_scores", JSON),
                    ],
                    True,
                )
            ),
        }
    )


def _detection_catalog(dbconn):
    from datapipe.compute import Catalog, Table
    from datapipe.store.database import TableStoreDB

    return Catalog(
        {
            "image_gt": Table(
                store=TableStoreDB(
                    dbconn,
                    "image_gt",
                    [
                        Column("image_id", String, primary_key=True),
                        Column("bboxes", JSON),
                        Column("labels", JSON),
                    ],
                    True,
                )
            ),
            "subset_has_image": Table(
                store=TableStoreDB(
                    dbconn,
                    "subset_has_image",
                    [
                        Column("image_id", String, primary_key=True),
                        Column("subset_id", String, primary_key=True),
                    ],
                    True,
                )
            ),
            "detection_prediction": Table(
                store=TableStoreDB(
                    dbconn,
                    "detection_prediction",
                    [
                        Column("image_id", String, primary_key=True),
                        Column("detection_model_id", String, primary_key=True),
                        Column("bboxes", JSON),
                        Column("labels", JSON),
                        Column("prediction__detection_scores", JSON),
                    ],
                    True,
                )
            ),
        }
    )


def _classification_catalog(dbconn):
    from datapipe.compute import Catalog, Table
    from datapipe.store.database import TableStoreDB

    return Catalog(
        {
            "image_gt": Table(
                store=TableStoreDB(
                    dbconn,
                    "image_gt",
                    [
                        Column("image_id", String, primary_key=True),
                        Column("label", String),
                    ],
                    True,
                )
            ),
            "subset_has_image": Table(
                store=TableStoreDB(
                    dbconn,
                    "subset_has_image",
                    [
                        Column("image_id", String, primary_key=True),
                        Column("subset_id", String, primary_key=True),
                    ],
                    True,
                )
            ),
            "classification_prediction": Table(
                store=TableStoreDB(
                    dbconn,
                    "classification_prediction",
                    [
                        Column("image_id", String, primary_key=True),
                        Column("classification_model_id", String, primary_key=True),
                        Column("prediction__top_n", Integer, primary_key=True),
                        Column("label", String),
                        Column("prediction__classification_score", Float),
                    ],
                    True,
                )
            ),
        }
    )


def _keypoints_catalog(dbconn):
    from datapipe.compute import Catalog, Table
    from datapipe.store.database import TableStoreDB

    return Catalog(
        {
            "image_gt": Table(
                store=TableStoreDB(
                    dbconn,
                    "image_gt",
                    [
                        Column("image_id", String, primary_key=True),
                        Column("bboxes", JSON),
                        Column("labels", JSON),
                        Column("keypoints", JSON),
                    ],
                    True,
                )
            ),
            "subset_has_image": Table(
                store=TableStoreDB(
                    dbconn,
                    "subset_has_image",
                    [
                        Column("image_id", String, primary_key=True),
                        Column("subset_id", String, primary_key=True),
                    ],
                    True,
                )
            ),
            "keypoints_model": Table(
                store=TableStoreDB(
                    dbconn,
                    "keypoints_model",
                    [
                        Column("keypoints_model_id", String, primary_key=True),
                    ],
                    True,
                )
            ),
            "keypoints_prediction": Table(
                store=TableStoreDB(
                    dbconn,
                    "keypoints_prediction",
                    [
                        Column("image_id", String, primary_key=True),
                        Column("keypoints_model_id", String, primary_key=True),
                        Column("bboxes", JSON),
                        Column("labels", JSON),
                        Column("keypoints", JSON),
                        Column("prediction__detection_scores", JSON),
                        Column("prediction__keypoints_scores", JSON),
                    ],
                    True,
                )
            ),
        }
    )


def _total_labels_catalog(dbconn):
    from datapipe.compute import Catalog, Table
    from datapipe.store.database import TableStoreDB

    return Catalog(
        {
            "item_gt": Table(
                store=TableStoreDB(
                    dbconn,
                    "item_gt",
                    [
                        Column("item_id", String, primary_key=True),
                        Column("label", String),
                    ],
                    True,
                )
            ),
            "subset_has_item": Table(
                store=TableStoreDB(
                    dbconn,
                    "subset_has_item",
                    [
                        Column("item_id", String, primary_key=True),
                        Column("subset_id", String, primary_key=True),
                    ],
                    True,
                )
            ),
        }
    )


def _build_pipeline_metrics_step():
    from datapipe_ml.workflows.detection_classification.metrics import CountMetrics_Subset_PipelineModel

    return CountMetrics_Subset_PipelineModel(
        input__image__ground_truth="image_gt",
        input__subset__has__image="subset_has_image",
        input__pipeline_prediction="pipeline_prediction",
        output__pipeline_model__metrics_on__image="pipeline_metrics_on_image",
        output__pipeline_model__metrics_by_cls_on__subset="pipeline_metrics_by_cls",
        output__pipeline_model__metrics_on__subset="pipeline_metrics_on_subset",
        primary_keys=["image_id"],
        bbox_id__name=None,
        create_table=True,
    )


def _build_detection_metrics_step():
    from datapipe_ml.tasks.detection.metrics import CountMetrics_Subset_DetectionModel

    return CountMetrics_Subset_DetectionModel(
        input__image__ground_truth="image_gt",
        input__subset__has__image="subset_has_image",
        input__detection_prediction="detection_prediction",
        output__detection_model__metrics__on__image="detection_metrics_on_image",
        output__detection_model__metrics__on__subset="detection_metrics_on_subset",
        primary_keys=["image_id"],
        bbox_id__name=None,
        create_table=True,
    )


def _build_classification_metrics_step():
    from datapipe_ml.tasks.classification.metrics import CountMetrics_Subset_ClassificationModel

    return CountMetrics_Subset_ClassificationModel(
        input__image__ground_truth="image_gt",
        input__subset__has__image="subset_has_image",
        input__classification_prediction="classification_prediction",
        output__classification_model__metrics__on__image="classification_metrics_on_image",
        output__classification_model__metrics_by_cls_on__subset="classification_metrics_by_cls",
        output__classification_model__metrics_on__subset="classification_metrics_on_subset",
        primary_keys=["image_id"],
        create_table=True,
    )


def _build_keypoints_metrics_step():
    from datapipe_ml.tasks.keypoints.metrics import CountMetrics_Subset_KeypointsModel

    return CountMetrics_Subset_KeypointsModel(
        input__image__ground_truth="image_gt",
        input__subset__has__image="subset_has_image",
        input__keypoints_model="keypoints_model",
        input__keypoints_prediction="keypoints_prediction",
        output__keypoints_model__metrics__on__subset="keypoints_metrics_on_subset",
        primary_keys=["image_id"],
        bbox_id__name=None,
        create_table=True,
    )


def _build_total_labels_step():
    from datapipe_ml.statistics.total import CountTotalLabelOnSubset

    return CountTotalLabelOnSubset(
        input__item__ground_truth="item_gt",
        input__subset__has__item="subset_has_item",
        output__subset__has__label__total="subset_label_total",
        primary_keys=["item_id"],
        create_table=True,
    )


METRICS_INNER_JOIN_CASES = [
    pytest.param(
        _pipeline_catalog,
        _build_pipeline_metrics_step,
        {"subset_has_image", "pipeline_prediction"},
        id="pipeline",
    ),
    pytest.param(
        _detection_catalog,
        _build_detection_metrics_step,
        {"subset_has_image", "detection_prediction"},
        id="detection",
    ),
    pytest.param(
        _classification_catalog,
        _build_classification_metrics_step,
        {"subset_has_image", "classification_prediction"},
        id="classification",
    ),
    pytest.param(
        _keypoints_catalog,
        _build_keypoints_metrics_step,
        {"subset_has_image", "keypoints_prediction"},
        id="keypoints",
    ),
    pytest.param(
        _total_labels_catalog,
        _build_total_labels_step,
        {"subset_has_item"},
        id="total-labels",
    ),
]


@pytest.mark.parametrize(
    ("catalog_factory", "step_factory", "required_tables"),
    METRICS_INNER_JOIN_CASES,
)
def test_subset_metrics_use_inner_join_for_required_inputs(
    base_datastore,
    dbconn,
    catalog_factory,
    step_factory,
    required_tables,
):
    _require_metrics_runtime()
    from datapipe.compute import Pipeline, build_compute

    ds = base_datastore
    catalog = catalog_factory(dbconn)
    steps = build_compute(ds, catalog, Pipeline([step_factory()]))
    step = _first_batch_transform_step(steps)
    _assert_inner_join_inputs(step, required_tables)


ORPHAN_PREDICTION_CASES = [
    pytest.param(
        _pipeline_catalog,
        _build_pipeline_metrics_step,
        "pipeline_prediction",
        "pipeline_metrics_on_image",
        "pipeline_metrics_on_subset",
        id="pipeline",
    ),
    pytest.param(
        _detection_catalog,
        _build_detection_metrics_step,
        "detection_prediction",
        "detection_metrics_on_image",
        "detection_metrics_on_subset",
        id="detection",
    ),
    pytest.param(
        _classification_catalog,
        _build_classification_metrics_step,
        "classification_prediction",
        "classification_metrics_on_image",
        "classification_metrics_on_subset",
        id="classification",
    ),
    pytest.param(
        _keypoints_catalog,
        _build_keypoints_metrics_step,
        "keypoints_prediction",
        None,
        "keypoints_metrics_on_subset",
        id="keypoints",
    ),
]


@pytest.mark.parametrize(
    (
        "catalog_factory",
        "step_factory",
        "prediction_table",
        "per_image_output",
        "subset_output",
    ),
    ORPHAN_PREDICTION_CASES,
)
def test_orphan_prediction_without_subset_is_skipped(
    base_datastore,
    dbconn,
    catalog_factory,
    step_factory,
    prediction_table,
    per_image_output,
    subset_output,
):
    _require_metrics_runtime()
    from datapipe.compute import Pipeline, build_compute, run_steps

    ds = base_datastore
    catalog = catalog_factory(dbconn)

    if catalog_factory is _classification_catalog:
        catalog.get_datatable(ds, "image_gt").store_chunk(
            pd.DataFrame({"image_id": ["i_valid"], "label": ["cat"]})
        )
        catalog.get_datatable(ds, "subset_has_image").store_chunk(
            pd.DataFrame({"image_id": ["i_valid"], "subset_id": ["val"]})
        )
        catalog.get_datatable(ds, prediction_table).store_chunk(
            pd.DataFrame(
                {
                    "image_id": ["i_valid", "i_orphan"],
                    "classification_model_id": ["m1", "m1"],
                    "prediction__top_n": [1, 1],
                    "label": ["cat", "cat"],
                    "prediction__classification_score": [0.9, 0.8],
                }
            )
        )
    elif catalog_factory is _detection_catalog:
        catalog.get_datatable(ds, "image_gt").store_chunk(
            pd.DataFrame({"image_id": ["i_valid"], "bboxes": [[[1, 1, 10, 10]]], "labels": [["cat"]]})
        )
        catalog.get_datatable(ds, "subset_has_image").store_chunk(
            pd.DataFrame({"image_id": ["i_valid"], "subset_id": ["val"]})
        )
        catalog.get_datatable(ds, prediction_table).store_chunk(
            pd.DataFrame(
                {
                    "image_id": ["i_valid", "i_orphan"],
                    "detection_model_id": ["m1", "m1"],
                    "bboxes": [[[1, 1, 10, 10]], [[2, 2, 8, 8]]],
                    "labels": [["cat"], ["cat"]],
                    "prediction__detection_scores": [[0.95], [0.9]],
                }
            )
        )
    elif catalog_factory is _keypoints_catalog:
        keypoints = [[[1, 1], [10, 1], [10, 10], [1, 10]]]
        catalog.get_datatable(ds, "image_gt").store_chunk(
            pd.DataFrame(
                {
                    "image_id": ["i_valid"],
                    "bboxes": [[[1, 1, 10, 10]]],
                    "labels": [["cat"]],
                    "keypoints": [keypoints],
                }
            )
        )
        catalog.get_datatable(ds, "subset_has_image").store_chunk(
            pd.DataFrame({"image_id": ["i_valid"], "subset_id": ["val"]})
        )
        catalog.get_datatable(ds, "keypoints_model").store_chunk(
            pd.DataFrame({"keypoints_model_id": ["m1"]})
        )
        catalog.get_datatable(ds, prediction_table).store_chunk(
            pd.DataFrame(
                {
                    "image_id": ["i_valid", "i_orphan"],
                    "keypoints_model_id": ["m1", "m1"],
                    "bboxes": [[[1, 1, 10, 10]], [[2, 2, 8, 8]]],
                    "labels": [["cat"], ["cat"]],
                    "keypoints": [keypoints, keypoints],
                    "prediction__detection_scores": [[0.95], [0.9]],
                    "prediction__keypoints_scores": [[[0.9, 0.9, 0.9, 0.9]], [[0.8, 0.8, 0.8, 0.8]]],
                }
            )
        )
    else:
        catalog.get_datatable(ds, "image_gt").store_chunk(
            pd.DataFrame({"image_id": ["i_valid"], "bboxes": [[[1, 1, 10, 10]]], "labels": [["cat"]]})
        )
        catalog.get_datatable(ds, "subset_has_image").store_chunk(
            pd.DataFrame({"image_id": ["i_valid"], "subset_id": ["val"]})
        )
        catalog.get_datatable(ds, prediction_table).store_chunk(
            pd.DataFrame(
                {
                    "image_id": ["i_valid", "i_orphan"],
                    "detection_model_id": ["det1", "det1"],
                    "classification_model_id": ["cls1", "cls1"],
                    "bboxes": [[[1, 1, 10, 10]], [[2, 2, 8, 8]]],
                    "labels": [["cat"], ["cat"]],
                    "prediction__detection_scores": [[0.95], [0.9]],
                }
            )
        )

    steps = build_compute(ds, catalog, Pipeline([step_factory()]))
    run_steps(ds, steps)

    overall = ds.get_table(subset_output).get_data()
    assert len(overall) == 1
    assert overall.iloc[0]["subset_id"] == "val"

    if per_image_output is not None:
        per_image = ds.get_table(per_image_output).get_data()
        assert set(per_image["image_id"]) == {"i_valid"}


def test_frozen_dataset_keypoints_metrics_use_inner_join(base_datastore, dbconn):
    _require_metrics_runtime()
    from datapipe.compute import Catalog, Pipeline, Table, build_compute
    from datapipe.store.database import TableStoreDB

    from datapipe_ml.tasks.keypoints.metrics import CountMetrics_FrozenDataset_KeypointsModel

    catalog = Catalog(
        {
            "keypoints_frozen_dataset__has__image_gt": Table(
                store=TableStoreDB(
                    dbconn,
                    "keypoints_frozen_dataset__has__image_gt",
                    [
                        Column("image_id", String, primary_key=True),
                        Column("keypoints_frozen_dataset_id", String, primary_key=True),
                        Column("subset_id", String, primary_key=True),
                        Column("bboxes", JSON),
                        Column("labels", JSON),
                        Column("keypoints", JSON),
                    ],
                    True,
                )
            ),
            "keypoints_model": Table(
                store=TableStoreDB(
                    dbconn,
                    "keypoints_model",
                    [Column("keypoints_model_id", String, primary_key=True)],
                    True,
                )
            ),
            "keypoints_prediction": Table(
                store=TableStoreDB(
                    dbconn,
                    "keypoints_prediction",
                    [
                        Column("image_id", String, primary_key=True),
                        Column("keypoints_model_id", String, primary_key=True),
                        Column("bboxes", JSON),
                        Column("labels", JSON),
                        Column("keypoints", JSON),
                    ],
                    True,
                )
            ),
        }
    )
    steps = build_compute(
        base_datastore,
        catalog,
        Pipeline(
            [
                CountMetrics_FrozenDataset_KeypointsModel(
                    input__keypoints_frozen_dataset__has__image_gt="keypoints_frozen_dataset__has__image_gt",
                    input__keypoints_model="keypoints_model",
                    input__keypoints_prediction="keypoints_prediction",
                    output__keypoints_model__metrics_on__frozen_dataset="keypoints_metrics_on_frozen_dataset",
                    primary_keys=["image_id"],
                    bbox_id__name=None,
                    create_table=True,
                )
            ]
        ),
    )
    step = _first_batch_transform_step(steps)
    _assert_inner_join_inputs(
        step,
        {"keypoints_frozen_dataset__has__image_gt", "keypoints_prediction"},
    )


def test_orphan_item_without_subset_is_skipped_for_total_labels(base_datastore, dbconn):
    _require_metrics_runtime()
    from datapipe.compute import Pipeline, build_compute, run_steps

    ds = base_datastore
    catalog = _total_labels_catalog(dbconn)
    catalog.get_datatable(ds, "item_gt").store_chunk(
        pd.DataFrame({"item_id": ["i_valid", "i_orphan"], "label": ["cat", "dog"]})
    )
    catalog.get_datatable(ds, "subset_has_item").store_chunk(
        pd.DataFrame({"item_id": ["i_valid"], "subset_id": ["val"]})
    )

    steps = build_compute(ds, catalog, Pipeline([_build_total_labels_step()]))
    run_steps(ds, steps)

    totals = ds.get_table("subset_label_total").get_data()
    assert set(totals["subset_id"]) == {"val"}
    assert set(totals["label"]) == {"cat"}
    assert totals.loc[totals["label"] == "cat", "label__total"].iloc[0] == 1
