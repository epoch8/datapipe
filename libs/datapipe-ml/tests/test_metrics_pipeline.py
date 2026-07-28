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


def test_pipeline_metrics_for_perfect_prediction(base_datastore, dbconn):
    _require_metrics_runtime()
    from datapipe.compute import Pipeline, build_compute, run_steps

    from datapipe_ml.workflows.detection_classification.metrics import (
        CountMetrics_Subset_PipelineModel,
    )

    ds = base_datastore
    catalog = _make_catalog(dbconn)
    catalog.get_datatable(ds, "image_gt").store_chunk(
        pd.DataFrame({"image_id": ["i1"], "bboxes": [[[1, 1, 10, 10]]], "labels": [["cat"]]})
    )
    catalog.get_datatable(ds, "subset_has_image").store_chunk(pd.DataFrame({"image_id": ["i1"], "subset_id": ["val"]}))
    catalog.get_datatable(ds, "pipeline_prediction").store_chunk(
        pd.DataFrame(
            {
                "image_id": ["i1"],
                "detection_model_id": ["det1"],
                "classification_model_id": ["cls1"],
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
                CountMetrics_Subset_PipelineModel(
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
            ]
        ),
    )
    run_steps(ds, steps)

    overall = ds.get_table("pipeline_metrics_on_subset").get_data()
    assert len(overall) == 1
    row = overall.iloc[0]
    assert row["calc__support"] == 1
    assert row["calc__accuracy"] == pytest.approx(1.0)
    assert row["calc__weighted_f1_score"] == pytest.approx(1.0)

    per_image = ds.get_table("pipeline_metrics_on_image").get_data()
    from datapipe_ml.metrics.common import METRICS_NULL_LABEL, is_metrics_null_label

    null_rows = per_image[per_image["label"].map(is_metrics_null_label)]
    assert len(null_rows) == 1
    assert null_rows.iloc[0]["calc__TP"] == 1
    assert null_rows.iloc[0]["calc__FP"] == 0

    by_cls = ds.get_table("pipeline_metrics_by_cls").get_data()
    assert METRICS_NULL_LABEL not in set(by_cls["label"].tolist())
