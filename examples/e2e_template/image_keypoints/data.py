from __future__ import annotations

from cv_pipeliner.utils.fiftyone import FifyOneSession
from datapipe_ml.utils.image_data_stores import FiftyOneImagesDataTableStore
from datapipe.compute import Catalog, Table
from datapipe.store.database import TableStoreDB
from sqlalchemy import Column, Float, JSON, String

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
        "keypoints_model": Table(
            TableStoreDB(
                dbconn=DBCONN,
                name="keypoints_model",
                data_sql_schema=[
                    Column("keypoints_model_id", String(), primary_key=True),
                    Column("keypoints_model__type", String()),
                    Column("keypoints_model__model_path", String()),
                    Column("keypoints_model__input_size", JSON),
                    Column("keypoints_model__class_names", JSON),
                    Column("keypoints_model__score_threshold", Float),
                ],
            )
        ),
        "keypoints_models": Table(
            TableStoreDB(
                dbconn=DBCONN,
                name="keypoints_models",
                data_sql_schema=[
                    Column("keypoints_model_id", String(), primary_key=True),
                    Column("keypoints_model__type", String()),
                    Column("keypoints_model__model_path", String()),
                    Column("keypoints_model__input_size", JSON),
                    Column("keypoints_model__class_names", JSON),
                    Column("keypoints_model__score_threshold", Float),
                ],
            )
        ),
        "keypoints_model_train": Table(
            TableStoreDB(
                dbconn=DBCONN,
                name="keypoints_model_train",
                data_sql_schema=[
                    Column("keypoints_model_id", String(), primary_key=True),
                    Column("keypoints_model__type", String()),
                    Column("keypoints_model__model_path", String()),
                    Column("keypoints_model__input_size", JSON),
                    Column("keypoints_model__class_names", JSON),
                    Column("keypoints_model__score_threshold", Float),
                ],
            )
        ),
        "best_keypoints_model": Table(
            TableStoreDB(
                dbconn=DBCONN,
                name="best_keypoints_model",
                data_sql_schema=[
                    Column("keypoints_model_id", String(), primary_key=True),
                    Column("keypoints_model__type", String()),
                    Column("keypoints_model__model_path", String()),
                    Column("keypoints_model__input_size", JSON),
                    Column("keypoints_model__class_names", JSON),
                    Column("keypoints_model__score_threshold", Float),
                ],
            )
        ),
        "images_with_predictions": Table(
            TableStoreDB(
                dbconn=DBCONN,
                name="images_with_predictions",
                data_sql_schema=[
                    Column("image_name", String(), primary_key=True),
                    Column("prediction", JSON),
                    Column("keypoints_model_id", String()),
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
                    Column("keypoints", JSON),
                    Column("keypoints_visibility", JSON),
                    Column("flip_idx", JSON),
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
                fo_detections_label="predictions_bbox",
                fo_keypoints_label="predictions_keypoints",
                rm_only_fo_fields=True,
                primary_schema=[Column("image_name", String(255), primary_key=True)],
            )
        ),
        "fiftyone_annotations": Table(
            FiftyOneImagesDataTableStore(
                dataset=FIFTYONE_DATASET_NAME,
                fo_session=fo_session,
                fo_detections_label="annotations_bbox",
                fo_keypoints_label="annotations_keypoints",
                rm_only_fo_fields=True,
                primary_schema=[Column("image_name", String(255), primary_key=True)],
            )
        ),
    }
)
