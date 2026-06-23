from __future__ import annotations

from datapipe.compute import Table
from datapipe.store.database import TableStoreDB
from sqlalchemy import Column, Float, Integer, JSON, String

from config import DBCONN

local_images_tbl = Table(
    name="local_images",
    store=TableStoreDB(
        dbconn=DBCONN,
        name="local_images",
        data_sql_schema=[
            Column("image_id", String, primary_key=True),
            Column("image_path", String),
        ],
        create_table=True,
    ),
)

sam_config_tbl = Table(
    name="sam_config",
    store=TableStoreDB(
        dbconn=DBCONN,
        name="sam_config",
        data_sql_schema=[
            Column("config_id", String, primary_key=True),
            Column("text_prompt", String),
        ],
        create_table=True,
    ),
)

sam_predictions_tbl = Table(
    name="sam_predictions",
    store=TableStoreDB(
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
    ),
)

sam_cvat_xml_tbl = Table(
    name="sam_cvat_xml",
    store=TableStoreDB(
        dbconn=DBCONN,
        name="sam_cvat_xml",
        data_sql_schema=[
            Column("image_id", String, primary_key=True),
            Column("annotations", String),
        ],
        create_table=True,
    ),
)

image_tbl = Table(
    name="image",
    store=TableStoreDB(
        dbconn=DBCONN,
        name="image",
        data_sql_schema=[
            Column("image_id", String, primary_key=True),
            Column("task_queue_id", String, primary_key=True),
            Column("image_path", String),
            Column("annotations", String),
        ],
        create_table=True,
    ),
)

image_annotations_tbl = Table(
    name="image__annotations",
    store=TableStoreDB(
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
    ),
)
