from __future__ import annotations

from cv_pipeliner.utils.fiftyone import FifyOneSession
from datapipe.compute import Table
from datapipe.store.database import TableStoreDB
from datapipe.store.filedir import PILFile, TableStoreFiledir
from datapipe_ml.utils.image_data_stores import FiftyOneImagesDataTableStore
from sqlalchemy import JSON, Column, String, Text

from config import (
    COMPOSITES_DIR,
    DBCONN,
    ENABLED_ENGINES,
    ENGINE_REGISTRY,
    ENGINE_TYPE_UNSTRUCTURED,
    FIFTYONE_DATASET_NAME,
    fo_det_field,
    fo_text_field,
    is_unstructured_engine,
)

fo_session = FifyOneSession()

images_tbl = Table(
    name="images",
    store=TableStoreDB(
        dbconn=DBCONN,
        name="images",
        data_sql_schema=[
            Column("image_id", String(255), primary_key=True),
            Column("kind", String(64)),
            Column("image_path", String(2048)),
        ],
        create_table=True,
    ),
)

engines_tbl = Table(
    name="engines",
    store=TableStoreDB(
        dbconn=DBCONN,
        name="engines",
        data_sql_schema=[
            Column("engine_id", String(64), primary_key=True),
            Column("engine_type", String(64)),
            Column("provider", String(64)),
            Column("model", String(255)),
        ],
        create_table=True,
    ),
)

ocr_results_tbl = Table(
    name="ocr_results",
    store=TableStoreDB(
        dbconn=DBCONN,
        name="ocr_results",
        data_sql_schema=[
            Column("image_id", String(255), primary_key=True),
            Column("engine_id", String(64), primary_key=True),
            Column("engine_type", String(64)),
            Column("boxes", JSON),
            Column("texts", JSON),
            Column("scores", JSON),
            Column("structured_json", Text),
            Column("full_text", Text),
        ],
        create_table=True,
    ),
)

fo_tables: dict[str, Table] = {}
for engine_id in ENABLED_ENGINES:
    fo_tables[engine_id] = Table(
        name=f"fo_{engine_id}",
        store=FiftyOneImagesDataTableStore(
            dataset=FIFTYONE_DATASET_NAME,
            fo_session=fo_session,
            fo_detections_label=fo_det_field(engine_id) if is_unstructured_engine(engine_id) else None,
            additional_info_keys_in_sample=[fo_text_field(engine_id)],
            additional_info_keys_in_fo_detections=["engine_id"],
            rm_only_fo_fields=True,
            primary_schema=[Column("image_id", String(255), primary_key=True)],
        ),
    )

composites_tbl = Table(
    name="composites",
    store=TableStoreFiledir(
        str(COMPOSITES_DIR / "{engine_id}" / "{image_id}.jpg"),
        PILFile("jpg"),
        primary_schema=[
            Column("engine_id", String(64), primary_key=True),
            Column("image_id", String(255), primary_key=True),
        ],
    ),
)

UNSTRUCTURED_ENGINE_IDS = [
    engine_id for engine_id in ENABLED_ENGINES if ENGINE_REGISTRY[engine_id]["type"] == ENGINE_TYPE_UNSTRUCTURED
]
