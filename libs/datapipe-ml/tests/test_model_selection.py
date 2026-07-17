from __future__ import annotations

import pandas as pd
import pytest
from sqlalchemy import Column, Float
from sqlalchemy.sql.sqltypes import String


def _require_datapipe_runtime():
    pytest.importorskip("tqdm")
    pytest.importorskip("datapipe")


def _make_catalog(dbconn):
    from datapipe.compute import Catalog, Table
    from datapipe.store.database import TableStoreDB

    return Catalog(
        {
            "model": Table(
                store=TableStoreDB(
                    dbconn,
                    "model",
                    [
                        Column("model_id", String, primary_key=True),
                        Column("group_id", String),
                    ],
                    True,
                )
            ),
            "model_metrics": Table(
                store=TableStoreDB(
                    dbconn,
                    "model_metrics",
                    [
                        Column("model_id", String, primary_key=True),
                        Column("subset_id", String, primary_key=True),
                        Column("metric", Float),
                    ],
                    True,
                )
            ),
        }
    )


def _run_find_best(ds, catalog, *, func="max", threshold=None, group_by=None):
    from datapipe.compute import Pipeline, build_compute, run_steps

    from datapipe_ml.metrics.model_selection import FindBestModel

    steps = build_compute(
        ds,
        catalog,
        Pipeline(
            [
                FindBestModel(
                    input__model="model",
                    input__model__metrics_on__subset="model_metrics",
                    output__attr__model__is_best="attr__model__is_best",
                    output__best_model="best_model",
                    subset_id="val",
                    is_best__name="model__is_best",
                    primary_keys=["model_id"],
                    metric__name="metric",
                    func=func,
                    create_table=True,
                    threshold=threshold,
                    group_by=group_by,
                )
            ]
        ),
    )
    run_steps(ds, steps)


def test_find_best_model_marks_max_metric(base_datastore, dbconn):
    _require_datapipe_runtime()
    catalog = _make_catalog(dbconn)
    ds = base_datastore
    catalog.get_datatable(ds, "model").store_chunk(pd.DataFrame({"model_id": ["m1", "m2"], "group_id": ["g", "g"]}))
    catalog.get_datatable(ds, "model_metrics").store_chunk(
        pd.DataFrame({"model_id": ["m1", "m2"], "subset_id": ["val", "val"], "metric": [0.1, 0.9]})
    )

    _run_find_best(ds, catalog)

    best = ds.get_table("best_model").get_data()
    assert best["model_id"].tolist() == ["m2"]


def test_find_best_model_threshold_can_reject_best(base_datastore, dbconn):
    _require_datapipe_runtime()
    catalog = _make_catalog(dbconn)
    ds = base_datastore
    catalog.get_datatable(ds, "model").store_chunk(pd.DataFrame({"model_id": ["m1", "m2"], "group_id": ["g", "g"]}))
    catalog.get_datatable(ds, "model_metrics").store_chunk(
        pd.DataFrame({"model_id": ["m1", "m2"], "subset_id": ["val", "val"], "metric": [0.1, 0.4]})
    )

    _run_find_best(ds, catalog, threshold=0.5)

    assert ds.get_table("best_model").get_data().empty


def test_find_best_model_can_choose_min_metric(base_datastore, dbconn):
    _require_datapipe_runtime()
    catalog = _make_catalog(dbconn)
    ds = base_datastore
    catalog.get_datatable(ds, "model").store_chunk(pd.DataFrame({"model_id": ["m1", "m2"], "group_id": ["g", "g"]}))
    catalog.get_datatable(ds, "model_metrics").store_chunk(
        pd.DataFrame({"model_id": ["m1", "m2"], "subset_id": ["val", "val"], "metric": [0.1, 0.9]})
    )

    _run_find_best(ds, catalog, func="min")

    best = ds.get_table("best_model").get_data()
    assert best["model_id"].tolist() == ["m1"]
