from __future__ import annotations

import pandas as pd
import pytest
from sqlalchemy import JSON, Column
from sqlalchemy.sql.sqltypes import String


def _require_metrics_runtime():
    pytest.importorskip("tqdm")
    pytest.importorskip("datapipe")
    pytest.importorskip("cv_pipeliner")
    pytest.importorskip("ultralytics")


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
    keypoints = [[[10, 10], [20, 10], [15, 20], [10, 30], [20, 30]]]
    catalog.get_datatable(ds, "image_gt").store_chunk(
        pd.DataFrame(
            {
                "image_id": ["i1"],
                "bboxes": [[[0, 0, 40, 40]]],
                "labels": [["cat"]],
                "keypoints": [keypoints],
            }
        )
    )
    catalog.get_datatable(ds, "subset_has_image").store_chunk(pd.DataFrame({"image_id": ["i1"], "subset_id": ["val"]}))
    catalog.get_datatable(ds, "keypoints_prediction").store_chunk(
        pd.DataFrame(
            {
                "image_id": ["i1"],
                "keypoints_model_id": ["m1"],
                "bboxes": [[[0, 0, 40, 40]]],
                "labels": [["cat"]],
                "keypoints": [keypoints],
                "prediction__detection_scores": [[0.95]],
                "prediction__keypoints_scores": [[[0.9, 0.9, 0.9, 0.9, 0.9]]],
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
                    input__keypoints_prediction="keypoints_prediction",
                    output__keypoints_model__metrics_on__image="keypoints_metrics_on_image",
                    output__keypoints_model__metrics_by_cls_on__subset="keypoints_metrics_by_cls",
                    output__keypoints_model__metrics_on__subset="keypoints_metrics_on_subset",
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
    assert row["calc__accuracy"] == pytest.approx(1.0)
    assert row["calc__pose_support"] == 1
    assert row["calc__pose_P"] == pytest.approx(1.0)
    assert row["calc__pose_R"] == pytest.approx(1.0)
    assert row["calc__pose_mAP50"] == pytest.approx(0.995, abs=0.01)
    assert row["calc__pose_mAP50_95"] == pytest.approx(0.995, abs=0.01)

    by_cls = ds.get_table("keypoints_metrics_by_cls").get_data()
    assert len(by_cls) >= 1
    assert (by_cls["calc__TP"] > 0).any()
