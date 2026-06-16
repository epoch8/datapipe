from __future__ import annotations

from cv_pipeliner.utils.fiftyone import FifyOneSession
from datapipe.compute import Catalog, Table
from datapipe.store.database import TableStoreDB
from datapipe.store.filedir import TableStoreFiledir
from datapipe_ml.utils.image_data_stores import FiftyOneImagesDataTableStore, NumpyDataFile
from sqlalchemy import JSON, Column, Integer, String

from config import DBCONN, EMBEDDINGS_DIR, FIFTYONE_DATASET_NAME

fo_session = FifyOneSession()

catalog = Catalog(
    {
        "local_images": Table(
            TableStoreDB(
                dbconn=DBCONN,
                name="local_images",
                data_sql_schema=[
                    Column("image_name", String(255), primary_key=True),
                    Column("image_path", String(2048)),
                ],
                create_table=True,
            )
        ),
        "image_labels": Table(
            TableStoreDB(
                dbconn=DBCONN,
                name="image_labels",
                data_sql_schema=[
                    Column("image_name", String(255), primary_key=True),
                    Column("label", String(255)),
                ],
                create_table=True,
            )
        ),
        "embedder": Table(
            TableStoreDB(
                dbconn=DBCONN,
                name="embedder",
                data_sql_schema=[
                    Column("embedder_id", String(255), primary_key=True),
                    Column("cls", String(255)),
                    Column("init_kwargs", JSON),
                    Column("batch_size", Integer),
                    Column("max_size", Integer),
                ],
                create_table=True,
            )
        ),
        "embeddings": Table(
            TableStoreFiledir(
                str(EMBEDDINGS_DIR / "{embedder_id}" / "{image_name}.npy"),
                adapter=NumpyDataFile(),
                primary_schema=[
                    Column("embedder_id", String(255), primary_key=True),
                    Column("image_name", String(255), primary_key=True),
                ],
            )
        ),
        "fiftyone_samples": Table(
            FiftyOneImagesDataTableStore(
                dataset=FIFTYONE_DATASET_NAME,
                fo_session=fo_session,
                fo_classification_label="ground_truth",
                rm_only_fo_fields=True,
                primary_schema=[Column("image_name", String(255), primary_key=True)],
            )
        ),
        "brain_status": Table(
            TableStoreDB(
                dbconn=DBCONN,
                name="brain_status",
                data_sql_schema=[
                    Column("embedder_id", String(255), primary_key=True),
                    Column("n_samples", Integer),
                    Column("vis_brain_key", String(255)),
                    Column("sim_brain_key", String(255)),
                    Column("status", String(64)),
                ],
                create_table=True,
            )
        ),
    }
)
