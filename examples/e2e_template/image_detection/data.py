from __future__ import annotations

from cv_pipeliner.utils.fiftyone import FifyOneSession
from datapipe_ml.utils.image_data_stores import FiftyOneImagesDataTableStore
from datapipe.compute import Catalog, Table
from datapipe.store.database import TableStoreDB
from sqlalchemy import Column, Float, Integer, JSON, String

from config import DBCONN, FIFTYONE_DATASET_NAME

fo_session = FifyOneSession()

catalog = Catalog(
    {
        "s3_images": Table(
            TableStoreDB(
                dbconn=DBCONN,
                name="s3_images",
                data_sql_schema=[
                    Column("image_name", String(), primary_key=True),
                    Column("image_url", String()),
                ],
            )
        ),
        "detection_model": Table(
            TableStoreDB(
                dbconn=DBCONN,
                name="detection_model",
                data_sql_schema=[
                    Column("detection_model_id", String(), primary_key=True),
                    Column("detection_model__type", String()),
                    Column("detection_model__model_path", String()),
                    Column("detection_model__input_size", JSON),
                    Column("detection_model__class_names", JSON),
                    Column("detection_model__score_threshold", Float),
                ],
            )
        ),
        "best_detection_model": Table(
            TableStoreDB(
                dbconn=DBCONN,
                name="best_detection_model",
                data_sql_schema=[
                    Column("detection_model_id", String(), primary_key=True),
                ],
            )
        ),
        "detection_prediction": Table(
            TableStoreDB(
                dbconn=DBCONN,
                name="detection_prediction",
                data_sql_schema=[
                    Column("image_name", String(), primary_key=True),
                    Column("detection_model_id", String(), primary_key=True),
                    Column("bboxes", JSON),
                    Column("labels", JSON),
                    Column("prediction__detection_scores", JSON),
                ],
            )
        ),
        "images_with_predictions": Table(
            TableStoreDB(
                dbconn=DBCONN,
                name="images_with_predictions",
                data_sql_schema=[
                    Column("image_name", String(), primary_key=True),
                    Column("detection_model_id", String(), primary_key=True),
                    Column("prediction", JSON),
                ],
            )
        ),
        "image__ground_truth": Table(
            TableStoreDB(
                dbconn=DBCONN,
                name="image__ground_truth",
                data_sql_schema=[
                    Column("image_name", String, primary_key=True),
                    Column("bboxes", JSON),
                    Column("labels", JSON),
                ],
            )
        ),
        "image__subset": Table(
            TableStoreDB(
                dbconn=DBCONN,
                name="image__subset",
                data_sql_schema=[
                    Column("image_name", String, primary_key=True),
                    Column("subset_id", String, primary_key=True),
                ],
            )
        ),
        "sec__image_without_ground_truth": Table(
            TableStoreDB(
                dbconn=DBCONN,
                name="sec__image_without_ground_truth",
                data_sql_schema=[
                    Column("image_name", String(), primary_key=True),
                ],
            )
        ),
        "local_images": Table(
            TableStoreDB(
                dbconn=DBCONN,
                name="local_images",
                data_sql_schema=[
                    Column("image_name", String(255), primary_key=True),
                    Column("local_path", String(1024)),
                ],
            )
        ),
        "fiftyone_predictions": Table(
            FiftyOneImagesDataTableStore(
                dataset=FIFTYONE_DATASET_NAME,
                fo_session=fo_session,
                fo_detections_label="predictions",
                rm_only_fo_fields=True,
                primary_schema=[Column("image_name", String(255), primary_key=True)],
            )
        ),
        "fiftyone_annotations": Table(
            FiftyOneImagesDataTableStore(
                dataset=FIFTYONE_DATASET_NAME,
                fo_session=fo_session,
                fo_detections_label="annotations",
                rm_only_fo_fields=True,
                primary_schema=[Column("image_name", String(255), primary_key=True)],
            )
        ),
    }
)
