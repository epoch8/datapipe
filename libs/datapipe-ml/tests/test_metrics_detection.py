from __future__ import annotations

import pandas as pd
import pytest
from sqlalchemy import JSON, Column
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


def test_detection_metrics_for_perfect_prediction(base_datastore, dbconn):
    _require_metrics_runtime()
    from datapipe.compute import Pipeline, build_compute, run_steps

    from datapipe_ml.tasks.detection.metrics import CountMetrics_Subset_DetectionModel

    ds = base_datastore
    catalog = _make_catalog(dbconn)
    catalog.get_datatable(ds, "image_gt").store_chunk(
        pd.DataFrame({"image_id": ["i1"], "bboxes": [[[1, 1, 10, 10]]], "labels": [["cat"]]})
    )
    catalog.get_datatable(ds, "subset_has_image").store_chunk(pd.DataFrame({"image_id": ["i1"], "subset_id": ["val"]}))
    catalog.get_datatable(ds, "detection_prediction").store_chunk(
        pd.DataFrame(
            {
                "image_id": ["i1"],
                "detection_model_id": ["m1"],
                "bboxes": [[[1, 1, 10, 10]]],
                "labels": [["cat"]],
                "prediction__detection_scores": [[0.95]],
            }
        )
    )

    steps = build_compute(
        ds,
        catalog,
        Pipeline(
            [
                CountMetrics_Subset_DetectionModel(
                    input__image__ground_truth="image_gt",
                    input__subset__has__image="subset_has_image",
                    input__detection_prediction="detection_prediction",
                    output__detection_model__metrics__on__image="detection_metrics_on_image",
                    output__detection_model__metrics__on__subset="detection_metrics_on_subset",
                    primary_keys=["image_id"],
                    bbox_id__name=None,
                    create_table=True,
                )
            ]
        ),
    )
    run_steps(ds, steps)

    overall = ds.get_table("detection_metrics_on_subset").get_data()
    assert len(overall) == 1
    row = overall.iloc[0]
    assert row["calc__support"] == 1
    assert row["calc__TP"] == 1
    assert row["calc__FP"] == 0
    assert row["calc__FN"] == 0
    assert row["calc__precision"] == pytest.approx(1.0)
    assert row["calc__recall"] == pytest.approx(1.0)
