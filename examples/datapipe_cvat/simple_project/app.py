import os
from pathlib import Path

import fsspec
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

from examples.datapipe_core._sqlite import sqlite_connstr

s3_bucket = os.environ.get("CVAT_EXAMPLE_S3_BUCKET", "s3://datapipe-cvat-example")
cvat_url = os.environ.get("CVAT_URL", "http://localhost:8080")
cvat_credentials = (
    os.environ.get("CVAT_USERNAME", "admin"),
    os.environ.get("CVAT_PASSWORD", "admin"),
)
cvat_project_id = int(os.environ.get("CVAT_PROJECT_ID", "1"))
cloud_storage_bucket = os.environ.get("CVAT_EXAMPLE_CLOUD_STORAGE_BUCKET", s3_bucket)
organization_slug = os.environ.get("CVAT_ORGANIZATION", "")
db_sqlite_path = Path("db.sqlite")
dbconn = DBConn(sqlite_connstr(str(db_sqlite_path)))

catalog = Catalog({
    "input_images": Table(
        name="input_images",
        store=TableStoreFiledir("input/{image_id}.jpeg", PILFile("jpg"), add_filepath_column=True),
    ),
    "image": Table(
        store=TableStoreDB(
            dbconn=dbconn,
            name="image",
            data_sql_schema=[
                Column("image_id", String, primary_key=True),
                Column("task_queue_id", String, primary_key=True),
                Column("image__s3_image_path", String),
                Column("annotations", String),
            ],
        )
    ),
})
image_id_to_annotations = {
    "01.jpeg": (
        '''
        <image>
        <polygon label="pipe_keypoints" source="manual" occluded="0" points="207.81,299.06;665.74,244.98;648.84,490.00;658.98,665.74;684.32,767.12;506.90,809.37;339.61,831.33;246.67,836.40;223.02,741.78;226.40,503.52" z_order="0">
            </polygon>
        </image>
        '''
    ),
    "02.jpeg": (
        '''
        <image>
        <polygon label="document" source="manual" occluded="0" points="172.33,195.98;439.31,184.15;442.69,393.68;412.27,873.58;528.87,882.03;199.36,910.75;211.19,756.98;197.67,721.50;187.53,527.18;187.53,385.24;170.63,192.60" z_order="0">
        </polygon>
        <polygon label="document" source="manual" occluded="0" points="441.00,175.70;816.13,185.84;795.85,397.06;800.92,528.87;780.64,794.16;768.81,841.47;768.81,902.30;412.27,868.51;420.72,748.54;434.24,604.91;442.69,537.31;442.69,376.79" z_order="0">
        </polygon>
        </image>
        '''
    ),
    "03.jpeg": (
        '''
        <image>
        <box label="price_tag" source="manual" occluded="0" xtl="714.74" ytl="251.74" xbr="865.13" ybr="319.33" z_order="0">
        </box>
        <box label="price_tag" source="manual" occluded="0" xtl="413.96" ytl="653.91" xbr="540.69" ybr="704.60" z_order="0">
        </box>
        <box label="pipe" source="manual" occluded="0" xtl="104.73" ytl="386.93" xbr="909.06" ybr="581.25" z_order="0">
        </box>
        <box label="document" source="manual" occluded="0" xtl="165.57" ytl="179.08" xbr="601.53" ybr="917.51" z_order="0">
        </box>
        </image>
        '''
    ),
    "04.jpeg": (
        '''
        <image>
        <box label="cup" source="manual" occluded="0" xtl="581.25" ytl="258.50" xbr="941.17" ybr="711.36" z_order="0">
        </box>
        <box label="cup" source="manual" occluded="0" xtl="45.59" ytl="312.58" xbr="429.17" ybr="751.92" z_order="0">
        </box>
        </image>
        '''
    ),
}
image_id_to_annotations["05.jpeg"] = "<image></image>"
image_id_to_annotations["06.jpeg"] = "<image></image>"
image_id_to_annotations["07.jpeg"] = "<image></image>"
image_id_to_annotations["08.jpeg"] = "<image></image>"
image_id_to_annotations["09.jpeg"] = "<image></image>"
image_id_to_annotations["10.jpeg"] = "<image></image>"


def write_image_to_s3(df: pd.DataFrame) -> pd.DataFrame:
    df["image__s3_image_path"] = df.apply(
        lambda row: f"{s3_bucket.rstrip('/')}/datapipe-cvat-test/{Path(row['filepath']).name}", axis=1
    )
    for filepath, image__s3_image_path in zip(df["filepath"], df["image__s3_image_path"]):
        with fsspec.open(filepath, "rb") as src:
            with fsspec.open(image__s3_image_path, "wb") as dst:
                dst.write(src.read())
    df["annotations"] = df.apply(lambda row: image_id_to_annotations[Path(row["filepath"]).name], axis=1)
    df["task_queue_id"] = df["image_id"].apply(lambda x: "queue1" if x < "05.jpeg" else "queue2")
    return df[["image_id", "task_queue_id", "image__s3_image_path", "annotations"]]


pipeline = Pipeline(
    [
        UpdateExternalTable(output="input_images"),
        BatchTransform(
            write_image_to_s3,
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
            file_path_column="image__s3_image_path",
            labels=[("stage", "cvat")],
            minimum_files_in_job=2,
            files_batch=2,
            cvat_url=cvat_url,
            cvat_credentials=cvat_credentials,
            cvat_project_id=cvat_project_id,
            cvat_organization=organization_slug,
            primary_keys=["image_id", "task_queue_id"],
            cloud_storage_bucket=cloud_storage_bucket,
            task_queue_id__name="task_queue_id",
            task_name_format="[{date:%Y-%m-%d}] TaskQueue:{task_queue_id} batch:{inner_task_id}",
        )
    ]
)


ds = DataStore(dbconn)
app = DatapipeApp(ds, catalog, pipeline)
