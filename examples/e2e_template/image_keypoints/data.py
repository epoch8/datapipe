from __future__ import annotations

from datapipe_ml.utils.image_data_stores import FiftyOneImagesDataTableStore
from datapipe.compute import Catalog, Table
from datapipe.store.database import TableStoreDB
from datapipe.store.table_store import TableStore
from sqlalchemy import Column, Float, JSON, String

from examples.e2e_template.common import ServiceSettings
from examples.e2e_template.image_keypoints.config import KEYPOINTS_LABELS


def build_catalog(settings: ServiceSettings, *, include_fiftyone: bool = True) -> Catalog:
    dbconn = settings.dbconn
    if include_fiftyone:
        from cv_pipeliner.utils.fiftyone import FifyOneSession

        fo_session = FifyOneSession()
        fiftyone_predictions_store: TableStore = FiftyOneImagesDataTableStore(
            dataset="datapipe_keypoints_e2e",
            fo_session=fo_session,
            fo_detections_label="predictions_bbox",
            fo_keypoints_label="predictions_keypoints",
            rm_only_fo_fields=True,
            primary_schema=[Column("image_name", String(255), primary_key=True)],
        )
        fiftyone_annotations_store: TableStore = FiftyOneImagesDataTableStore(
            dataset="datapipe_keypoints_e2e",
            fo_session=fo_session,
            fo_detections_label="annotations_bbox",
            fo_keypoints_label="annotations_keypoints",
            rm_only_fo_fields=True,
            primary_schema=[Column("image_name", String(255), primary_key=True)],
        )
    else:
        fiftyone_predictions_store = TableStoreDB(
            dbconn=dbconn,
            name="fiftyone_predictions",
            data_sql_schema=[Column("image_name", String(255), primary_key=True), Column("image_data", JSON)],
            create_table=True,
        )
        fiftyone_annotations_store = TableStoreDB(
            dbconn=dbconn,
            name="fiftyone_annotations",
            data_sql_schema=[Column("image_name", String(255), primary_key=True), Column("image_data", JSON)],
            create_table=True,
        )
    return Catalog(
        {
            "s3_images": Table(
                TableStoreDB(
                    dbconn=dbconn,
                    name="s3_images",
                    data_sql_schema=[
                        Column("image_name", String(), primary_key=True),
                        Column("image_url", String()),
                    ],
                    create_table=True,
                )
            ),
            "keypoints_model": Table(
                TableStoreDB(
                    dbconn=dbconn,
                    name="keypoints_model",
                    data_sql_schema=[
                        Column("keypoints_model_id", String(), primary_key=True),
                        Column("keypoints_model__type", String()),
                        Column("keypoints_model__model_path", String()),
                        Column("keypoints_model__input_size", JSON),
                        Column("keypoints_model__class_names", JSON),
                        Column("keypoints_model__score_threshold", Float),
                    ],
                    create_table=True,
                )
            ),
            "images_with_predictions": Table(
                TableStoreDB(
                    dbconn=dbconn,
                    name="images_with_predictions",
                    data_sql_schema=[
                        Column("image_name", String(), primary_key=True),
                        Column("prediction", JSON),
                        Column("keypoints_model_id", String()),
                    ],
                    create_table=True,
                )
            ),
            "image__ground_truth": Table(
                TableStoreDB(
                    dbconn=dbconn,
                    name="image__ground_truth",
                    data_sql_schema=[
                        Column("image_name", String, primary_key=True),
                        Column("bboxes", JSON),
                        Column("keypoints", JSON),
                        Column("keypoints_visibility", JSON),
                        Column("flip_idx", JSON),
                        Column("labels", JSON),
                    ],
                    create_table=True,
                )
            ),
            "image__subset": Table(
                TableStoreDB(
                    dbconn=dbconn,
                    name="image__subset",
                    data_sql_schema=[
                        Column("image_name", String, primary_key=True),
                        Column("subset_id", String, primary_key=True),
                    ],
                    create_table=True,
                )
            ),
            "keypoints_predictions": Table(
                TableStoreDB(
                    dbconn=dbconn,
                    name="keypoints_predictions",
                    data_sql_schema=[
                        Column("image_name", String, primary_key=True),
                        Column("keypoints_model_id", String, primary_key=True),
                        Column("bboxes", JSON),
                        Column("keypoints", JSON),
                        Column("labels", JSON),
                        Column("prediction__detection_scores", JSON),
                        Column("prediction__keypoints_scores", JSON),
                    ],
                    create_table=True,
                )
            ),
            "local_images": Table(
                TableStoreDB(
                    dbconn=dbconn,
                    name="local_images",
                    data_sql_schema=[
                        Column("image_name", String(255), primary_key=True),
                        Column("local_path", String(1024)),
                    ],
                    create_table=True,
                )
            ),
            "fiftyone_predictions": Table(
                fiftyone_predictions_store
            ),
            "fiftyone_annotations": Table(
                fiftyone_annotations_store
            ),
        }
    )

