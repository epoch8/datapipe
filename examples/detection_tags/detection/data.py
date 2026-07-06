from __future__ import annotations

from datapipe.compute import Catalog, Table
from datapipe.store.database import TableStoreDB
from sqlalchemy import Column, Float, Integer, JSON, String

from config import DBCONN


def _t(name, schema):
    return Table(TableStoreDB(dbconn=DBCONN, name=name, data_sql_schema=schema, create_table=True))


catalog = Catalog(
    {
        # a load request: add a row here, then run `datapipe step --labels=stage=load run`
        "load_request": _t("load_request", [
            Column("request_id", String, primary_key=True),
            Column("n", Integer),
            Column("offset", Integer),
            Column("tag", String),
            Column("darken", Float),
        ]),
        # images uploaded by the load step (image_name = object basename)
        "s3_images": _t("s3_images", [
            Column("image_name", String, primary_key=True),
            Column("image_url", String),
        ]),
        # ground truth injected by the data-loading script (no Label Studio)
        "image__ground_truth": _t("image__ground_truth", [
            Column("image_name", String, primary_key=True),
            Column("bboxes", JSON),
            Column("labels", JSON),
        ]),
        # train/val split
        "image__subset": _t("image__subset", [
            Column("image_name", String, primary_key=True),
            Column("subset_id", String, primary_key=True),
        ]),
        # --- tags -------------------------------------------------------------
        "tag": _t("tag", [
            Column("tag_id", String, primary_key=True),
            Column("name", String),
        ]),
        "image__tag": _t("image__tag", [
            Column("image_name", String, primary_key=True),
            Column("tag_id", String, primary_key=True),
        ]),
        # per-image metrics produced by the CountMetrics step (declared so the
        # tag-metrics transform can read it as an input)
        "detection_model_train__metrics_on_image": _t("detection_model_train__metrics_on_image", [
            Column("image_name", String, primary_key=True),
            Column("detection_model_id", String, primary_key=True),
            Column("subset_id", String, primary_key=True),
            Column("calc__support", Integer),
            Column("calc__TP", Integer),
            Column("calc__FP", Integer),
            Column("calc__FN", Integer),
            Column("calc__iou_mean", Float),
        ]),
        # per-(model, tag, subset) metrics — the tag arc lives here
        "tag_metrics": _t("tag_metrics", [
            Column("detection_model_id", String, primary_key=True),
            Column("tag_id", String, primary_key=True),
            Column("subset_id", String, primary_key=True),
            Column("calc__images_support", Integer),
            Column("calc__support", Integer),
            Column("calc__precision", Float),
            Column("calc__recall", Float),
            Column("calc__f1_score", Float),
        ]),
    }
)
