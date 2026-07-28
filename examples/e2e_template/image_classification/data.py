from __future__ import annotations

from cv_pipeliner.utils.fiftyone import FifyOneSession
from datapipe.compute import Catalog, Table
from datapipe.store.database import TableStoreDB
from datapipe_ml.utils.image_data_stores import FiftyOneImagesDataTableStore
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
        "classification_model": Table(
            TableStoreDB(
                dbconn=DBCONN,
                name="classification_model",
                data_sql_schema=[
                    Column("classification_model_id", String(), primary_key=True),
                    Column("classification_model__type", String()),
                    Column("classification_model__model_path", String()),
                    Column("classification_model__input_size", JSON),
                    Column("classification_model__class_names", JSON),
                    Column("classification_model__preprocess_input_script_path", String()),
                ],
            )
        ),
        "best_classification_model": Table(
            TableStoreDB(
                dbconn=DBCONN,
                name="best_classification_model",
                data_sql_schema=[
                    Column("classification_model_id", String(), primary_key=True),
                ],
            )
        ),
        "classification_prediction": Table(
            TableStoreDB(
                dbconn=DBCONN,
                name="classification_prediction",
                data_sql_schema=[
                    Column("image_name", String(), primary_key=True),
                    Column("classification_model_id", String(), primary_key=True),
                    Column("prediction__top_n", Integer, primary_key=True),
                    Column("label", String()),
                    Column("prediction__classification_score", Float),
                ],
            )
        ),
        "ls_classification_prediction": Table(
            TableStoreDB(
                dbconn=DBCONN,
                name="ls_classification_prediction",
                data_sql_schema=[
                    Column("image_name", String(), primary_key=True),
                    Column("classification_model_id", String(), primary_key=True),
                    Column("prediction__top_n", Integer, primary_key=True),
                    Column("label", String()),
                    Column("prediction__classification_score", Float),
                ],
            )
        ),
        "images_with_predictions": Table(
            TableStoreDB(
                dbconn=DBCONN,
                name="images_with_predictions",
                data_sql_schema=[
                    Column("image_name", String(), primary_key=True),
                    Column("classification_model_id", String(), primary_key=True),
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
                    Column("label", String),
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
        "fiftyone_images": Table(
            FiftyOneImagesDataTableStore(
                dataset=FIFTYONE_DATASET_NAME,
                fo_session=fo_session,
                fo_detections_label="images",
                rm_only_fo_fields=False,
                primary_schema=[
                    Column("image_name", String(255), primary_key=True),
                ],
            )
        ),
        "fiftyone_annotations": Table(
            FiftyOneImagesDataTableStore(
                dataset=FIFTYONE_DATASET_NAME,
                fo_session=fo_session,
                fo_classification_label="annotations",
                rm_only_fo_fields=True,
                primary_schema=[
                    Column("image_name", String(255), primary_key=True),
                ],
                additional_info_keys_in_sample=["subset_id"],
            )
        ),
        "fiftyone_predictions_from_best_model": Table(
            FiftyOneImagesDataTableStore(
                dataset=FIFTYONE_DATASET_NAME,
                fo_session=fo_session,
                fo_classification_label="predictions_from_best_model",
                rm_only_fo_fields=True,
                primary_schema=[
                    Column("image_name", String(255), primary_key=True),
                ],
                additional_info_keys_in_sample=["classification_model_id"],
            )
        ),
    }
)
