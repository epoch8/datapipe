import os
from pathlib import Path

import pandas as pd
from datapipe_cvat.cvat_step import CVATStep
from sqlalchemy import Column
from sqlalchemy.sql.sqltypes import String

from datapipe.compute import Catalog, DatapipeApp, Pipeline, Table
from datapipe.datatable import DataStore
from datapipe.step.batch_transform import BatchTransform
from datapipe.step.update_external_table import UpdateExternalTable
from datapipe.store.database import DBConn, TableStoreDB
from datapipe.store.filedir import PILFile, TableStoreFiledir

cvat_url = os.environ.get("CVAT_URL", "http://localhost:8080")
cvat_credentials = (
    os.environ.get("CVAT_USERNAME", "admin"),
    os.environ.get("CVAT_PASSWORD", "admin"),
)
cvat_project_id = int(os.environ.get("CVAT_PROJECT_ID", "1"))
organization_slug = os.environ.get("CVAT_ORGANIZATION", "")
db_sqlite_path = Path("db.sqlite")
dbconn = DBConn(f"sqlite+pysqlite3:///{db_sqlite_path}", None)

ANNOTATIONS_DIR = Path("input/annotations")
FILES_BATCH = 5


def load_preannotation(image_path: Path) -> str:
    annotation_path = ANNOTATIONS_DIR / f"{image_path.stem}.xml"
    if annotation_path.exists():
        return annotation_path.read_text(encoding="utf-8")
    return "<image></image>"


catalog = Catalog({
    "input_images": Table(
        name="input_images",
        store=TableStoreFiledir("input/{image_id}.jpg", PILFile("jpg"), add_filepath_column=True),
    ),
    "image": Table(
        store=TableStoreDB(
            dbconn=dbconn,
            name="image",
            data_sql_schema=[
                Column("image_id", String, primary_key=True),
                Column("task_queue_id", String, primary_key=True),
                Column("image_path", String),
                Column("annotations", String),
            ],
        )
    ),
})


def prepare_images(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["image_path"] = df["filepath"].apply(lambda path: str(Path(path).resolve()))
    df["annotations"] = df["filepath"].apply(lambda path: load_preannotation(Path(path)))
    df["task_queue_id"] = "queue1"
    return df[["image_id", "task_queue_id", "image_path", "annotations"]]


pipeline = Pipeline(
    [
        UpdateExternalTable(output="input_images"),
        BatchTransform(
            prepare_images,
            inputs=["input_images"],
            outputs=["image"],
        ),
        CVATStep(
            input="image",
            output__input_batches="image_batches",
            output__cvat_task="cvat_task",
            output__cvat_files="cvat_images",
            task_sync_table="cvat_task_sync_table",
            output__cvat_annotation="cvat_annotation",
            file_path_column="image_path",
            labels=[("stage", "cvat")],
            minimum_files_in_job=FILES_BATCH,
            files_batch=FILES_BATCH,
            cvat_url=cvat_url,
            cvat_credentials=cvat_credentials,
            cvat_project_id=cvat_project_id,
            cvat_organization=organization_slug,
            primary_keys=["image_id", "task_queue_id"],
            cloud_storage_bucket=None,
            # If True, changed files are re-uploaded only when the existing CVAT frame
            # has no annotations; annotated frames are kept to avoid losing manual work.
            delete_unannotated_tasks_only_on_update=False,
            task_queue_id__name="task_queue_id",
            task_name_format="[{date:%Y-%m-%d}] TaskQueue:{task_queue_id} batch:{inner_task_id}",
        )
    ]
)


ds = DataStore(dbconn)
app = DatapipeApp(ds, catalog, pipeline)
