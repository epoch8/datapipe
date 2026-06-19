from __future__ import annotations

from dotenv import load_dotenv

load_dotenv()

from datapipe.compute import DatapipeApp, Pipeline
from datapipe.datatable import DataStore
from datapipe.executor import ExecutorConfig
from datapipe.step.batch_generate import BatchGenerate
from datapipe.step.batch_transform import BatchTransform
from datapipe_cvat.cvat_step import CVATStep

import steps
from config import (
    CVAT_ORGANIZATION,
    CVAT_PASSWORD,
    CVAT_PROJECT_ID,
    CVAT_URL,
    CVAT_USERNAME,
    DBCONN,
    FILES_BATCH,
    PRIMARY_KEYS,
)
from data import catalog

pipeline = Pipeline(
    [
        BatchGenerate(
            steps.list_local_images,
            outputs=["local_images"],
            labels=[("stage", "ingest")],
        ),
        BatchGenerate(
            steps.huggingface_login,
            outputs=["hf_auth"],
            labels=[("stage", "auth")],
        ),
        BatchTransform(
            func=steps.sam_inference,
            inputs=["local_images", "hf_auth"],
            outputs=["sam_predictions"],
            transform_keys=["image_id"],
            chunk_size=1,
            labels=[("stage", "sam")],
            executor_config=ExecutorConfig(parallelism=0),
        ),
        BatchTransform(
            func=steps.sam_to_cvat_xml,
            inputs=["sam_predictions"],
            outputs=["sam_cvat_xml"],
            transform_keys=["image_id"],
            labels=[("stage", "sam")],
        ),
        BatchTransform(
            func=steps.prepare_cvat_input,
            inputs=["local_images", "sam_cvat_xml"],
            outputs=["image"],
            transform_keys=["image_id"],
            labels=[("stage", "cvat")],
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
            minimum_files_in_job=1,
            files_batch=FILES_BATCH,
            cvat_url=CVAT_URL,
            cvat_credentials=(CVAT_USERNAME, CVAT_PASSWORD),
            cvat_project_id=CVAT_PROJECT_ID,
            cvat_organization=CVAT_ORGANIZATION,
            primary_keys=PRIMARY_KEYS,
            cloud_storage_bucket=None,
            delete_unannotated_tasks_only_on_update=False,
            task_queue_id__name="task_queue_id",
            task_name_format="[{date:%Y-%m-%d}] TaskQueue:{task_queue_id} batch:{inner_task_id}",
            create_table=True,
        ),
        BatchTransform(
            func=steps.parse_cvat_annotations,
            inputs=["cvat_annotation"],
            outputs=["image__annotations"],
            transform_keys=["image_id", "task_queue_id", "inner_task_id"],
            labels=[("stage", "cvat")],
        ),
    ]
)

ds = DataStore(DBCONN, create_meta_table=True)
app = DatapipeApp(ds, catalog, pipeline)
