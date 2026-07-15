from __future__ import annotations

from cv_pipeliner.utils.fiftyone import FifyOneSession
from datapipe.compute import Catalog, Table
from datapipe.store.database import TableStoreDB
from datapipe_ml.utils.image_data_stores import FiftyOneImagesDataTableStore
from sqlalchemy import Column, Float, Integer, JSON, String

from config import DBCONN, FIFTYONE_DATASET_NAME

fo_session = FifyOneSession()


def _t(name, schema):
    return Table(TableStoreDB(dbconn=DBCONN, name=name, data_sql_schema=schema))


def _fo_table(fo_detections_label: str, *, rm_only_fo_fields: bool = True,
              additional_info_keys_in_sample=()):
    return Table(FiftyOneImagesDataTableStore(
        dataset=FIFTYONE_DATASET_NAME,
        fo_session=fo_session,
        fo_detections_label=fo_detections_label,
        rm_only_fo_fields=rm_only_fo_fields,
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
        # tag dimension: tag_id is the tag NAME itself (text) + a readable description (two columns)
        "tag": _t("tag", [
            Column("tag_id", String, primary_key=True),
            Column("tag_description", String),
        ]),
        "image__tag": _t("image__tag", [
            Column("image_name", String, primary_key=True),
            Column("tag_id", String, primary_key=True),
        ]),
        # --- FiftyOne (stage=fiftyone) ---------------------------------------
        # Same layout as e2e_template/image_detection: one shared dataset, separate stores
        # per field. Baseline vs retrained predictions stay side-by-side for A/B comparison;
        # subset_id + tag_id are sample fields for filtering (e.g. tag_id=night).
        "local_images": _t("local_images", [
            Column("image_name", String(255), primary_key=True),
            Column("local_path", String(1024)),
        ]),
        "fiftyone_images": _fo_table("images", rm_only_fo_fields=False),
        "fiftyone_annotations": _fo_table(
            "annotations",
            additional_info_keys_in_sample=["subset_id", "tag_id"],
        ),
        "fiftyone_predictions_model_a": _fo_table(
            "predictions_model_a",
            additional_info_keys_in_sample=["detection_model_id_a"],
        ),
        "fiftyone_predictions_model_b": _fo_table(
            "predictions_model_b",
            additional_info_keys_in_sample=["detection_model_id_b"],
        ),
    }
)
