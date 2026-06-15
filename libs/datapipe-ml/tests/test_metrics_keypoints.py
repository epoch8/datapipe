from __future__ import annotations

import pandas as pd
import pytest
from sqlalchemy import JSON, Column, Float
from sqlalchemy.sql.sqltypes import String


def _require_metrics_runtime():
    pytest.importorskip("tqdm")
    pytest.importorskip("datapipe")
    pytest.importorskip("cv_pipeliner")


def _make_catalog(dbconn):
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
                        Column("keypoints_model__pose_P", Float),
                        Column("keypoints_model__pose_R", Float),
                        Column("keypoints_model__pose_mAP50", Float),
                        Column("keypoints_model__pose_mAP50_95", Float),
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


def test_keypoints_metrics_for_perfect_prediction(base_datastore, dbconn):
    _require_metrics_runtime()
    from datapipe.compute import Pipeline, build_compute, run_steps

    from datapipe_ml.tasks.keypoints.metrics import CountMetrics_Subset_KeypointsModel

    ds = base_datastore
    catalog = _make_catalog(dbconn)
    keypoints = [[[1, 1], [10, 1], [10, 10], [1, 10]]]
    catalog.get_datatable(ds, "image_gt").store_chunk(
        pd.DataFrame({"image_id": ["i1"], "bboxes": [[[1, 1, 10, 10]]], "labels": [["cat"]], "keypoints": [keypoints]})
    )
    catalog.get_datatable(ds, "subset_has_image").store_chunk(pd.DataFrame({"image_id": ["i1"], "subset_id": ["val"]}))
    catalog.get_datatable(ds, "keypoints_model").store_chunk(
        pd.DataFrame(
            {
                "keypoints_model_id": ["m1"],
            }
        )
    )
    catalog.get_datatable(ds, "keypoints_prediction").store_chunk(
        pd.DataFrame(
            {
                "image_id": ["i1"],
                "keypoints_model_id": ["m1"],
                "bboxes": [[[1, 1, 10, 10]]],
                "labels": [["cat"]],
                "keypoints": [keypoints],
                "prediction__detection_scores": [[0.95]],
                "prediction__keypoints_scores": [[[0.9, 0.9, 0.9, 0.9]]],
            }
        )
    )

    steps = build_compute(
        ds,
        catalog,
        Pipeline(
            [
                CountMetrics_Subset_KeypointsModel(
                    input__image__ground_truth="image_gt",
                    input__subset__has__image="subset_has_image",
                    input__keypoints_model="keypoints_model",
                    input__keypoints_prediction="keypoints_prediction",
                    output__keypoints_model__metrics__on__subset="keypoints_metrics_on_subset",
                    primary_keys=["image_id"],
                    bbox_id__name=None,
                    create_table=True,
                )
            ]
        ),
    )
    run_steps(ds, steps)

    overall = ds.get_table("keypoints_metrics_on_subset").get_data()
    assert len(overall) == 1
    row = overall.iloc[0]
    assert row["calc__support"] == 1
    assert row["calc__TP"] == 1
    assert row["calc__FP"] == 0
    assert row["calc__FN"] == 0
    assert row["calc__precision"] == pytest.approx(1.0)
    assert row["calc__recall"] == pytest.approx(1.0)
    assert pd.isna(row["calc__pose_mAP50"])


def test_pose_metrics_are_copied_from_model_row():
    from datapipe_ml.tasks.keypoints.metrics import _pose_metrics_from_model_row

    df__keypoints_model = pd.DataFrame(
        {
            "keypoints_model__pose_P": [0.91],
            "keypoints_model__pose_R": [0.82],
            "keypoints_model__pose_mAP50": [0.77],
            "keypoints_model__pose_mAP50_95": [0.55],
        }
    )

    assert _pose_metrics_from_model_row(df__keypoints_model) == {
        "calc__pose_P": 0.91,
        "calc__pose_R": 0.82,
        "calc__pose_mAP50": 0.77,
        "calc__pose_mAP50_95": 0.55,
    }
