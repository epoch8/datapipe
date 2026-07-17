from __future__ import annotations

import pandas as pd
import pytest
from sqlalchemy import Column, Float, Integer
from sqlalchemy.sql.sqltypes import String


def _require_datapipe_runtime():
    pytest.importorskip("tqdm")
    pytest.importorskip("datapipe")


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


def test_classification_metrics_for_correct_and_wrong_predictions(base_datastore, dbconn):
    _require_datapipe_runtime()
    from datapipe.compute import Pipeline, build_compute, run_steps

    from datapipe_ml.tasks.classification.metrics import (
        CountMetrics_Subset_ClassificationModel,
    )

    ds = base_datastore
    catalog = _make_catalog(dbconn)
    catalog.get_datatable(ds, "image_gt").store_chunk(pd.DataFrame({"image_id": ["i1", "i2"], "label": ["cat", "dog"]}))
    catalog.get_datatable(ds, "subset_has_image").store_chunk(
        pd.DataFrame({"image_id": ["i1", "i2"], "subset_id": ["val", "val"]})
    )
    catalog.get_datatable(ds, "classification_prediction").store_chunk(
        pd.DataFrame(
            {
                "image_id": ["i1", "i2"],
                "classification_model_id": ["m1", "m1"],
                "prediction__top_n": [1, 1],
                "label": ["cat", "cat"],
                "prediction__classification_score": [0.9, 0.6],
            }
        )
    )

    steps = build_compute(
        ds,
        catalog,
        Pipeline(
            [
                CountMetrics_Subset_ClassificationModel(
                    input__image__ground_truth="image_gt",
                    input__subset__has__image="subset_has_image",
                    input__classification_prediction="classification_prediction",
                    output__classification_model__metrics__on__image="classification_metrics_on_image",
                    output__classification_model__metrics_by_cls_on__subset="classification_metrics_by_cls",
                    output__classification_model__metrics_on__subset="classification_metrics_on_subset",
                    primary_keys=["image_id"],
                    create_table=True,
                )
            ]
        ),
    )
    run_steps(ds, steps)

    overall = ds.get_table("classification_metrics_on_subset").get_data()
    assert len(overall) == 1
    row = overall.iloc[0]
    assert row["classification_model_id"] == "m1"
    assert row["subset_id"] == "val"
    assert row["calc__images_support"] == 2
    assert row["calc__support"] == 2
    assert row["calc__accuracy"] == pytest.approx(1 / 3)

    by_cls = ds.get_table("classification_metrics_by_cls").get_data()
    assert set(by_cls["label"]) == {"cat", "dog"}
