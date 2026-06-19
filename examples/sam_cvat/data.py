from __future__ import annotations

from datapipe.compute import Catalog, Table
from datapipe.store.database import TableStoreDB
from sqlalchemy import Column, Float, Integer, JSON, String

from config import DBCONN, PRIMARY_KEYS, TASK_QUEUE_ID

catalog = Catalog(
    {
        "local_images": Table(
            TableStoreDB(
                dbconn=DBCONN,
                name="local_images",
                data_sql_schema=[
                    Column("image_id", String, primary_key=True),
                    Column("image_path", String),
                ],
                create_table=True,
            )
        ),
        "hf_auth": Table(
            TableStoreDB(
                dbconn=DBCONN,
                name="hf_auth",
                data_sql_schema=[
                    Column("auth_id", String, primary_key=True),
                    Column("auth_ok", String),
                ],
                create_table=True,
            )
        ),
        "sam_predictions": Table(
            TableStoreDB(
                dbconn=DBCONN,
                name="sam_predictions",
                data_sql_schema=[
                    Column("image_id", String, primary_key=True),
                    Column("detection_id", String, primary_key=True),
                    Column("score", Float),
                    Column("x_min", Float),
                    Column("y_min", Float),
                    Column("x_max", Float),
                    Column("y_max", Float),
                    Column("polygon_points", JSON),
                ],
                create_table=True,
            )
        ),
        "sam_cvat_xml": Table(
            TableStoreDB(
                dbconn=DBCONN,
                name="sam_cvat_xml",
                data_sql_schema=[
                    Column("image_id", String, primary_key=True),
                    Column("annotations", String),
                ],
                create_table=True,
            )
        ),
        "image": Table(
            TableStoreDB(
                dbconn=DBCONN,
                name="image",
                data_sql_schema=[
                    Column("image_id", String, primary_key=True),
                    Column("task_queue_id", String, primary_key=True),
                    Column("image_path", String),
                    Column("annotations", String),
                ],
                create_table=True,
            )
        ),
        "image__annotations": Table(
            TableStoreDB(
                dbconn=DBCONN,
                name="image__annotations",
                data_sql_schema=[
                    Column("image_id", String, primary_key=True),
                    Column("task_queue_id", String, primary_key=True),
                    Column("inner_task_id", Integer, primary_key=True),
                    Column("boxes", JSON),
                    Column("polygons", JSON),
                    Column("box_labels", JSON),
                    Column("polygon_labels", JSON),
                ],
                create_table=True,
            )
        ),
    }
)

# Re-export for steps that need task_queue_id default.
DEFAULT_TASK_QUEUE_ID = TASK_QUEUE_ID
DEFAULT_PRIMARY_KEYS = PRIMARY_KEYS
