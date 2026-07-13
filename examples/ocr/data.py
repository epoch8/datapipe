from __future__ import annotations

from cv_pipeliner.utils.fiftyone import FifyOneSession
from datapipe.compute import Table
from datapipe.store.database import TableStoreDB
from datapipe_ml.utils.image_data_stores import FiftyOneImagesDataTableStore
from sqlalchemy import Column, String, Text

from config import DBCONN, ENABLED_ENGINES, FIFTYONE_DATASET_NAME, fo_text_field

fo_session = FifyOneSession()

images_tbl = Table(
    name="images",
    store=TableStoreDB(
        dbconn=DBCONN,
        name="images",
        data_sql_schema=[
            Column("image_id", String(255), primary_key=True),
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
            Column("output_json", Text),
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
            additional_info_keys_in_sample=[fo_text_field(engine_id)],
            rm_only_fo_fields=True,
            primary_schema=[Column("image_id", String(255), primary_key=True)],
        ),
    )
