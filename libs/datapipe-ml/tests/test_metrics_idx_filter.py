"""RunConfig filters (e.g. training_request_id) are stamped onto idx but are not
columns of metrics-on-image tables. Subset aggregators must ignore those extras.
"""

from __future__ import annotations

import pandas as pd
import pytest
from sqlalchemy import Column, Float, Integer, MetaData, String, Table

from datapipe_ml.metrics.common import (
    METRICS_NULL_LABEL,
    idx_columns_present_on_table,
    idx_in_table_clause,
)

REQUEST_ID = "request_abc"
EXTRA_IDX_COL = "training_request_id"


def _require_datapipe():
    pytest.importorskip("tqdm")
    pytest.importorskip("datapipe")


def _idx_with_request(*rows: dict) -> pd.DataFrame:
    return pd.DataFrame([{**row, EXTRA_IDX_COL: REQUEST_ID} for row in rows])


# ---------------------------------------------------------------------------
# Helper unit tests
# ---------------------------------------------------------------------------


def test_idx_columns_present_on_table_skips_run_config_extra_filters():
    meta = MetaData()
    tbl = Table(
        "metrics_on_image",
        meta,
        Column("detection_model_id", String),
        Column("tag_id", String),
        Column("subset_id", String),
    )
    idx = _idx_with_request(
        {"detection_model_id": "m1", "tag_id": "night", "subset_id": "train"}
    )

    assert idx_columns_present_on_table(tbl, idx) == [
        "detection_model_id",
        "tag_id",
        "subset_id",
    ]
    clause = idx_in_table_clause(tbl, idx)
    compiled = str(clause.compile(compile_kwargs={"literal_binds": True}))
    assert EXTRA_IDX_COL not in compiled
    assert "m1" in compiled


def test_idx_in_table_clause_raises_when_no_overlap():
    meta = MetaData()
    tbl = Table("metrics_on_image", meta, Column("detection_model_id", String))
    idx = pd.DataFrame([{EXTRA_IDX_COL: REQUEST_ID}])

    with pytest.raises(ValueError, match="no overlap"):
        idx_in_table_clause(tbl, idx)


def test_idx_columns_present_preserves_idx_order():
    meta = MetaData()
    tbl = Table(
        "t",
        meta,
        Column("subset_id", String),
        Column("model_id", String),
        Column("tag_id", String),
    )
    idx = pd.DataFrame(
        [{"model_id": "m1", "tag_id": "t1", "subset_id": "val", EXTRA_IDX_COL: REQUEST_ID}]
    )
    assert idx_columns_present_on_table(tbl, idx) == ["model_id", "tag_id", "subset_id"]


# ---------------------------------------------------------------------------
# Integration: each subset aggregator with training_request_id on idx
# ---------------------------------------------------------------------------


def _make_table(ds, name: str, schema: list[Column]):
    from datapipe.store.database import TableStoreDB

    return ds.get_or_create_table(
        name,
        TableStoreDB(dbconn=ds.meta_dbconn, name=name, data_sql_schema=schema, create_table=True),
    )


def test_count_detection_metrics_on_subset_ignores_training_request_id(base_datastore):
    _require_datapipe()
    from datapipe.types import IndexDF

    from datapipe_ml.tasks.detection.metrics import count_detection_metrics_on_subset

    ds = base_datastore
    dt = _make_table(
        ds,
        "det_metrics_on_image",
        [
            Column("image_id", String, primary_key=True),
            Column("detection_model_id", String, primary_key=True),
            Column("subset_id", String, primary_key=True),
            Column("calc__support", Integer),
            Column("calc__TP", Integer),
            Column("calc__FP", Integer),
            Column("calc__FN", Integer),
            Column("calc__iou_mean", Float),
        ],
    )
    dt.store_chunk(
        pd.DataFrame(
            [
                {
                    "image_id": "i1",
                    "detection_model_id": "m1",
                    "subset_id": "val",
                    "calc__support": 2,
                    "calc__TP": 2,
                    "calc__FP": 0,
                    "calc__FN": 0,
                    "calc__iou_mean": 0.9,
                }
            ]
        )
    )

    idx = IndexDF(
        _idx_with_request({"detection_model_id": "m1", "subset_id": "val"})
    )
    out = count_detection_metrics_on_subset(
        ds,
        idx,
        [dt],
        kwargs={"detection_model_primary_keys": ["detection_model_id"]},
    )
    assert len(out) == 1
    assert out.iloc[0]["calc__TP"] == 2
    assert EXTRA_IDX_COL not in out.columns


def test_count_classification_metrics_on_subset_ignores_training_request_id(base_datastore):
    _require_datapipe()
    from datapipe.types import IndexDF

    from datapipe_ml.tasks.classification.metrics import count_classification_metrics_on_subset

    ds = base_datastore
    dt = _make_table(
        ds,
        "cls_metrics_on_image",
        [
            Column("image_id", String, primary_key=True),
            Column("classification_model_id", String, primary_key=True),
            Column("subset_id", String, primary_key=True),
            Column("label", String, primary_key=True),
            Column("calc__images_support", Integer),
            Column("calc__support", Integer),
            Column("calc__TP", Integer),
            Column("calc__FP", Integer),
            Column("calc__FN", Integer),
        ],
    )
    dt.store_chunk(
        pd.DataFrame(
            [
                {
                    "image_id": "i1",
                    "classification_model_id": "m1",
                    "subset_id": "val",
                    "label": "cat",
                    "calc__images_support": 1,
                    "calc__support": 1,
                    "calc__TP": 1,
                    "calc__FP": 0,
                    "calc__FN": 0,
                }
            ]
        )
    )

    idx = IndexDF(
        _idx_with_request({"classification_model_id": "m1", "subset_id": "val"})
    )
    df_by_cls, df_overall = count_classification_metrics_on_subset(
        ds,
        idx,
        [dt],
        kwargs={
            "classification_model_primary_keys": ["classification_model_id"],
            "pseudo_class_names": [],
        },
    )
    assert len(df_by_cls) == 1
    assert len(df_overall) == 1
    assert df_by_cls.iloc[0]["calc__TP"] == 1
    assert EXTRA_IDX_COL not in df_by_cls.columns
    assert EXTRA_IDX_COL not in df_overall.columns


def test_count_pipeline_metrics_on_subset_ignores_training_request_id(base_datastore):
    _require_datapipe()
    from datapipe.types import IndexDF

    from datapipe_ml.workflows.detection_classification.metrics import (
        count_pipeline_metrics_on_subset,
    )

    ds = base_datastore
    dt = _make_table(
        ds,
        "pipe_metrics_on_image",
        [
            Column("image_id", String, primary_key=True),
            Column("detection_model_id", String, primary_key=True),
            Column("subset_id", String, primary_key=True),
            Column("label", String, primary_key=True),
            Column("calc__images_support", Integer),
            Column("calc__support", Integer),
            Column("calc__TP", Integer),
            Column("calc__FP", Integer),
            Column("calc__FN", Integer),
            Column("calc__TP_extra_bbox", Integer),
            Column("calc__FP_extra_bbox", Integer),
            Column("calc__FN_extra_bbox", Integer),
        ],
    )
    dt.store_chunk(
        pd.DataFrame(
            [
                {
                    "image_id": "i1",
                    "detection_model_id": "m1",
                    "subset_id": "val",
                    "label": "cat",
                    "calc__images_support": 1,
                    "calc__support": 1,
                    "calc__TP": 1,
                    "calc__FP": 0,
                    "calc__FN": 0,
                    "calc__TP_extra_bbox": 0,
                    "calc__FP_extra_bbox": 0,
                    "calc__FN_extra_bbox": 0,
                },
                {
                    "image_id": "i1",
                    "detection_model_id": "m1",
                    "subset_id": "val",
                    "label": METRICS_NULL_LABEL,
                    "calc__images_support": 1,
                    "calc__support": 1,
                    "calc__TP": 1,
                    "calc__FP": 0,
                    "calc__FN": 0,
                    "calc__TP_extra_bbox": 0,
                    "calc__FP_extra_bbox": 0,
                    "calc__FN_extra_bbox": 0,
                },
            ]
        )
    )

    idx = IndexDF(
        _idx_with_request({"detection_model_id": "m1", "subset_id": "val"})
    )
    df_by_cls, df_overall = count_pipeline_metrics_on_subset(
        ds,
        idx,
        [dt],
        kwargs={
            "pipeline_model_primary_keys": ["detection_model_id"],
            "primary_keys": ["image_id"],
            "pseudo_class_names": [],
        },
    )
    assert len(df_by_cls) == 1
    assert len(df_overall) == 1
    assert df_by_cls.iloc[0]["label"] == "cat"
    assert EXTRA_IDX_COL not in df_by_cls.columns
    assert EXTRA_IDX_COL not in df_overall.columns


def test_count_keypoints_metrics_on_subset_ignores_training_request_id(base_datastore):
    """Regression for the KeyError 'training_request_id' in _aggregate_pose_on_subset."""
    _require_datapipe()
    pytest.importorskip("cv_pipeliner")
    from datapipe.types import IndexDF

    from datapipe_ml.tasks.keypoints.metrics import count_keypoints_metrics_on_subset

    ds = base_datastore
    dt = _make_table(
        ds,
        "kpt_metrics_on_image",
        [
            Column("image_id", String, primary_key=True),
            Column("keypoints_model_id", String, primary_key=True),
            Column("tag_id", String, primary_key=True),
            Column("subset_id", String, primary_key=True),
            Column("label", String, primary_key=True),
            Column("calc__images_support", Integer),
            Column("calc__support", Integer),
            Column("calc__TP", Integer),
            Column("calc__FP", Integer),
            Column("calc__FN", Integer),
            Column("calc__TP_extra_bbox", Integer),
            Column("calc__FP_extra_bbox", Integer),
            Column("calc__FN_extra_bbox", Integer),
            Column("calc__pose_support", Integer),
            Column("calc__pose_P", Float),
            Column("calc__pose_R", Float),
            Column("calc__pose_mAP50", Float),
            Column("calc__pose_mAP50_95", Float),
        ],
    )
    dt.store_chunk(
        pd.DataFrame(
            [
                {
                    "image_id": "i1",
                    "keypoints_model_id": "m1",
                    "tag_id": "tag_a",
                    "subset_id": "val",
                    "label": "cow",
                    "calc__images_support": 1,
                    "calc__support": 1,
                    "calc__TP": 1,
                    "calc__FP": 0,
                    "calc__FN": 0,
                    "calc__TP_extra_bbox": 0,
                    "calc__FP_extra_bbox": 0,
                    "calc__FN_extra_bbox": 0,
                    "calc__pose_support": None,
                    "calc__pose_P": None,
                    "calc__pose_R": None,
                    "calc__pose_mAP50": None,
                    "calc__pose_mAP50_95": None,
                },
                {
                    "image_id": "i1",
                    "keypoints_model_id": "m1",
                    "tag_id": "tag_a",
                    "subset_id": "val",
                    "label": METRICS_NULL_LABEL,
                    "calc__images_support": 1,
                    "calc__support": 1,
                    "calc__TP": 1,
                    "calc__FP": 0,
                    "calc__FN": 0,
                    "calc__TP_extra_bbox": 0,
                    "calc__FP_extra_bbox": 0,
                    "calc__FN_extra_bbox": 0,
                    "calc__pose_support": 2,
                    "calc__pose_P": 0.8,
                    "calc__pose_R": 0.7,
                    "calc__pose_mAP50": 0.75,
                    "calc__pose_mAP50_95": 0.6,
                },
            ]
        )
    )

    # Same shape as the production failure: transform keys + stamped request id.
    idx = IndexDF(
        _idx_with_request(
            {
                "keypoints_model_id": "m1",
                "tag_id": "tag_a",
                "subset_id": "val",
            }
        )
    )
    df_by_cls, df_overall = count_keypoints_metrics_on_subset(
        ds,
        idx,
        [dt],
        kwargs={
            "keypoints_model_primary_keys": ["keypoints_model_id", "tag_id"],
            "primary_keys": ["image_id"],
            "pseudo_class_names": [],
            "known_class_names": None,
            "has_threshold": False,
        },
    )
    assert len(df_by_cls) == 1
    assert len(df_overall) == 1
    assert df_overall.iloc[0]["calc__pose_support"] == 2
    assert pytest.approx(df_overall.iloc[0]["calc__pose_P"], rel=1e-6) == 0.8
    assert EXTRA_IDX_COL not in df_by_cls.columns
    assert EXTRA_IDX_COL not in df_overall.columns
