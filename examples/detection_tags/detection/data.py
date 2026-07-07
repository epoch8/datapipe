from __future__ import annotations

from cv_pipeliner.utils.fiftyone import FifyOneSession
from datapipe.compute import Catalog, Table
from datapipe.store.database import TableStoreDB
from datapipe_ml.utils.image_data_stores import FiftyOneImagesDataTableStore
from sqlalchemy import Column, Float, Integer, JSON, String

from config import DBCONN, FIFTYONE_DATASET_NAME

# one FiftyOne session/dataset shared by the ground-truth + per-model prediction stores;
# each store writes its own field (ground_truth / predictions_model_a / predictions_model_b)
# into the SAME dataset, so a sample shows GT and both models side by side.
fo_session = FifyOneSession()


def _t(name, schema):
    return Table(TableStoreDB(dbconn=DBCONN, name=name, data_sql_schema=schema, create_table=True))


def _fo(field, additional_info_keys_in_sample=()):
    return Table(FiftyOneImagesDataTableStore(
        dataset=FIFTYONE_DATASET_NAME,
        fo_session=fo_session,
        fo_detections_label=field,
        rm_only_fo_fields=True,
        additional_info_keys_in_sample=list(additional_info_keys_in_sample),
        primary_schema=[Column("image_name", String(255), primary_key=True)],
    ))


catalog = Catalog(
    {
        # a load request: add a row here, then run `datapipe step --labels=stage=load run`
        "load_request": _t("load_request", [
            Column("request_id", String, primary_key=True),
            Column("n", Integer),
            Column("offset", Integer),
            Column("tag", String),
            Column("darken", Float),
            # pin every image of this batch to a subset (train/val); NULL = let the
            # random split decide. Pinning is how the demo freezes val.
            Column("subset", String),
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
        # per-batch pinned subset emitted by the load step (--subset); the split step
        # honors these and only randomizes images without a hint. Freezes val.
        "image__subset_hint": _t("image__subset_hint", [
            Column("image_name", String, primary_key=True),
            Column("subset_id", String),
        ]),
        # --- tags -------------------------------------------------------------
        # tag dimension: numeric tag_id (surrogate key), tag_name (slug), tag_description (readable)
        "tag": _t("tag", [
            Column("tag_id", Integer, primary_key=True),
            Column("tag_name", String),
            Column("tag_description", String),
        ]),
        "image__tag": _t("image__tag", [
            Column("image_name", String, primary_key=True),
            Column("tag_id", Integer, primary_key=True),
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
            Column("tag_id", Integer, primary_key=True),
            Column("subset_id", String, primary_key=True),
            Column("calc__images_support", Integer),
            Column("calc__support", Integer),
            Column("calc__precision", Float),
            Column("calc__recall", Float),
            Column("calc__f1_score", Float),
        ]),
        # --- FiftyOne (stage=fiftyone) ---------------------------------------
        # images downloaded locally (FiftyOne renders from local files)
        "local_images": _t("local_images", [
            Column("image_name", String(255), primary_key=True),
            Column("local_path", String(1024)),
        ]),
        # GT boxes + per-sample subset/tag fields (filter tag=night in the FO App)
        "fiftyone_annotations": _fo("ground_truth", additional_info_keys_in_sample=["subset", "tag"]),
        # predictions of the baseline (model A) and the retrained model (model B)
        "fiftyone_predictions_model_a": _fo("predictions_model_a"),
        "fiftyone_predictions_model_b": _fo("predictions_model_b"),
    }
)
