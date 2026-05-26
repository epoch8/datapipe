from __future__ import annotations

import json
import logging
import re
import tempfile
import time
import xml.etree.ElementTree as ET
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Hashable, List, Literal, Optional, Tuple, Union

import pandas as pd
from cvat_sdk import Client as CVATClient
from cvat_sdk.api_client.exceptions import NotFoundException
from cvat_sdk.core.proxies.tasks import ResourceType, Task
from cvat_sdk.models import PatchedLabelRequest, TaskWriteRequest
from datapipe.compute import (
    Catalog,
    ComputeStep,
    DataStore,
    Pipeline,
    PipelineStep,
    Table,
    build_compute,
)
from datapipe.datatable import DataTable
from datapipe.executor import ExecutorConfig
from datapipe.run_config import RunConfig
from datapipe.step.batch_transform import BatchTransform
from datapipe.step.datatable_transform import DatatableTransform
from datapipe.store.database import TableStoreDB
from datapipe.types import (
    IndexDF,
    Labels,
    PipelineInput,
    PipelineOutput,
    data_to_index,
    get_pipeline_input_name,
    get_pipeline_output_name,
    index_difference,
)
from sqlalchemy import Column, DateTime, Integer, String, and_, func, select

from datapipe_cvat.utils import export_job_annotations, extract_key, get_cloud_storage

logger = logging.getLogger("datapipe.cvat")


class SkipError(Exception):
    """
    Exception when there is not enough data.
    """


def int_from_scalar(value: object) -> int:
    if isinstance(value, (int, float, str)):
        return int(value)
    raise TypeError(f"Expected int-like scalar, got {type(value)!r}")


def is_missing_scalar(value: object) -> bool:
    return value is None or value != value


def create_cvat_client(url: str, organization: str, credentials: Tuple[str, str]) -> CVATClient:
    client = CVATClient(url)
    client.organization_slug = organization
    client.login(credentials=credentials)
    return client


def open_new_batch(remaining_count: int, minimum_files_in_job_value: int, batch_idx: int, batch_fill: int):
    if remaining_count < minimum_files_in_job_value:
        return False, batch_idx, batch_fill
    batch_idx += 1
    batch_fill = 0
    return True, batch_idx, batch_fill


def assign_batches_to_files(
    df__item: pd.DataFrame,
    df__input_batches: pd.DataFrame,
    ds: DataStore,
    primary_keys: List[str],
    files_batch: Union[int, dict[Any, int]],
    minimum_files_in_job: Union[int, dict[Any, int]],
    output__input_batches: str,
    task_queue_id__name: str,
    sampling_order: Literal["default", "random"] = "default",
    sampling_random_seed: Optional[int] = None,
) -> pd.DataFrame:
    """
    Assigns an `inner_task_id` to each row, forming batches of images/videos for annotation.

    Extended behavior:
      - If `task_queue_id` (column name) is provided, batching is scoped per its value.
        Each distinct `task_queue_id` value has an independent sequence of `inner_task_id`
        and independent "open batch" fill tracking.
      - Otherwise, legacy global batching is used.

    Logic:
    1. Already assigned files are left untouched.
    2. New files are added to the last open batch if there is room.
    3. A new batch is created only if there are >= minimum_files_in_job unannotated files remaining in that scope.
    4. If there is insufficient data — the function does nothing and returns the current result.

    Arguments:
    :param df__item: Full set of images/videos to be assigned into batches.
    :param df__input_batches: Already assigned rows with `inner_task_id`.
    :param ds: Data source (Datapipe DataStore) containing the tables.
    :param primary_keys: List of keys uniquely identifying a row.
    :param files_batch: Maximum number of files in one batch.
    :param minimum_files_in_job: Minimum number of files required to start a new batch.
    :param output__input_batches: Name of the table containing the batch assignment results.
    :param task_queue_id__name: Column name that scopes batching (per-stream batching).

    :return: Updated DataFrame df__input_batches with assigned `inner_task_id` for new files.
    """

    if sampling_order == "random":
        df__item = df__item.sample(frac=1, replace=False, random_state=sampling_random_seed)

    logger.info(
        "assign_batches_to_files: incoming=%d, existing=%d, task_queue_id__name=%s",
        len(df__item),
        len(df__input_batches),
        task_queue_id__name,
    )

    existing_idx = data_to_index(df__input_batches, primary_keys)

    to_assign_df = index_difference(data_to_index(df__item, primary_keys), existing_idx)

    if to_assign_df.empty:
        logger.info("No new files to assign — skipping.")
        return df__input_batches

    dt__input_batches = ds.get_table(output__input_batches)
    assert isinstance(dt__input_batches.table_store, TableStoreDB)
    dt__input_batches_db_conn = dt__input_batches.table_store.dbconn.con
    dt__input_batches_data_table = dt__input_batches.table_store.data_table

    def _current_max_for(task_queue_id: Any) -> Tuple[int, int]:
        """
        Return (last_batch_idx, last_batch_fill) for the given scope.
        If no batches exist yet, return (-1, 0).
        """
        sql_max = select(func.max(dt__input_batches_data_table.c["inner_task_id"]))
        if task_queue_id__name is not None:
            sql_max = sql_max.where(dt__input_batches_data_table.c[task_queue_id__name] == task_queue_id)

        current_max: object = pd.read_sql(sql_max, con=dt__input_batches_db_conn).iat[0, 0]
        last_idx = int_from_scalar(current_max) if not is_missing_scalar(current_max) else -1
        if last_idx < 0:
            return -1, 0

        sql_fill = (
            select(func.count())
            .select_from(dt__input_batches_data_table)
            .where(dt__input_batches_data_table.c["inner_task_id"] == last_idx)
        )
        sql_fill = sql_fill.where(
            and_(
                dt__input_batches_data_table.c[task_queue_id__name] == task_queue_id,
                dt__input_batches_data_table.c["inner_task_id"] == last_idx,
            )
        )
        fill = int_from_scalar(pd.read_sql(sql_fill, con=dt__input_batches_db_conn).iat[0, 0])
        return last_idx, fill

    rows_to_insert: List[Dict[str, object]] = []

    for task_queue_id, df__remaining_rows in to_assign_df.groupby(task_queue_id__name, dropna=False):
        if isinstance(files_batch, dict):
            files_batch_value = files_batch[task_queue_id]
        else:
            files_batch_value = files_batch
        if isinstance(minimum_files_in_job, dict):
            minimum_files_in_job_value = minimum_files_in_job[task_queue_id]
        else:
            minimum_files_in_job_value = minimum_files_in_job

        batch_idx, batch_fill = _current_max_for(task_queue_id)

        if batch_idx >= 0:
            batch_fill = files_batch_value

        df__remaining_rows = df__remaining_rows.reset_index(drop=True)
        total_remaining_rows = len(df__remaining_rows)

        for i in range(total_remaining_rows):
            if batch_fill == files_batch_value or batch_idx == -1:
                opened, batch_idx, batch_fill = open_new_batch(
                    total_remaining_rows - i, minimum_files_in_job_value, batch_idx, batch_fill
                )
                if not opened:
                    break

            row_to_insert: Dict[str, object] = {key: df__remaining_rows.at[i, key] for key in primary_keys}
            row_to_insert[task_queue_id__name] = df__remaining_rows.at[i, task_queue_id__name]
            row_to_insert["inner_task_id"] = batch_idx
            rows_to_insert.append(row_to_insert)
            batch_fill += 1

    if not rows_to_insert:
        raise SkipError(f"Not enough new files to open a batch (need >= {minimum_files_in_job}). Skipping for now.")

    df_new = pd.DataFrame(rows_to_insert)
    if len(df_new) > 0:
        df__input_batches = pd.concat([df__input_batches, df_new], ignore_index=True)

    logger.info(
        "Assigned %d images into %d batches (scoped_by=%s)",
        len(df_new),
        df__input_batches["inner_task_id"].nunique(),
        task_queue_id__name,
    )

    return df__input_batches


def build_regex_from_format(
    task_name_format: Optional[str],
    inner_task_id: int,
    task_queue_id__name: str,
    task_queue_id: Any,
) -> re.Pattern:
    """
    Builds a regular expression from the task_name_format template,
    replacing placeholders with specific values or patterns.

    {inner_task_id} is replaced with its exact value,
    other placeholders (e.g., date) are replaced with '.*?'.
    """
    # Escape the base name immediately
    if task_name_format is None:
        raise ValueError("task_name_format must be provided")

    regex_str = re.escape(task_name_format)
    # Replace escaped placeholders with the desired patterns
    # 1. {task_queue_id__name}
    regex_str = regex_str.replace(re.escape(f"{{{task_queue_id__name}}}"), re.escape(str(task_queue_id)))
    # 2. {inner_task_id}
    regex_str = regex_str.replace(re.escape("{inner_task_id}"), re.escape(str(inner_task_id)))
    # 3. All other placeholders of the form {…}
    #    will be replaced with a non-greedy match of any text
    regex_str = re.sub(r"\\\{[^}]+\\\}", ".*?", regex_str)
    # Full match from start to end
    full_regex = f"^{regex_str}$"
    return re.compile(full_regex)


def _parse_api_exception(e: Exception) -> Tuple[Optional[dict], Optional[int]]:
    payload, task_id = None, None
    body = getattr(e, "body", None)
    logger.info(f"parse_api_exception: body: {body}")
    if body:
        try:
            payload = json.loads(body)
        except Exception:
            try:
                payload = json.loads(body.decode("utf-8"))
                logger.info(f"parse_api_exception: payload: {payload}")
            except Exception as e:
                payload = None
    if isinstance(payload, dict):
        op = payload.get("operation") or {}
        task_id = op.get("task_id")
    return payload, task_id

def _is_transient_failure(payload: Optional[dict]) -> bool:
    if not payload:
        return False
    logger.info(f"is_transient_failure: payload: {payload}")
    if payload.get("status") == "failed":
        msg = (payload.get("message") or "").lower()
        # самые частые транзиентные причины
        if "connection refused" in msg or "redis" in msg or "kvrocks" in msg or "timeout" in msg:
            return True
    return False

def _import_annotations_with_retry(
    task: Task,
    filename: str,
    *,
    format_name: str,
    max_attempts: int,
    attempt_poll_s: int
) -> None:
    """
    Импорт аннотаций с ретраями на транзиентные ошибки CVAT (включая кейсы, когда HTTP=200, а в теле status=failed).
    """
    for attempt in range(1, max_attempts + 1):
        try:
            logger.info(f"import_annotations_with_retry: attempt: {attempt}")
            task.import_annotations(format_name=format_name, filename=filename)
            logger.info(f"import_annotations_with_retry: attempt: {attempt}: success")
            return
        except Exception as e:
            logger.info(f"import_annotations_with_retry: attempt: {attempt}: exception: {e}")
            payload, _ = _parse_api_exception(e)
            transient = _is_transient_failure(payload)
            if attempt == max_attempts or not transient:
                raise
            logger.warning(
                "Transient CVAT error on annotations import for task %s (attempt %d/%d): %s; retry in %ds",
                getattr(task, "id", "?"), attempt, max_attempts, getattr(e, "reason", repr(e)), attempt_poll_s
            )
            time.sleep(attempt_poll_s)

def _annotations_are_empty(ann: Any) -> bool:
    if ann is None:
        return False
    length = max(len(ann.get("shapes", [])), len(ann.get("tracks", [])), len(ann.get("tags", [])))
    return (length == 0)

def get_or_create_task(
    df__batch: pd.DataFrame,
    file_path_column: str,
    cvat_client: CVATClient,
    project_id: int,
    inner_task_id: int,
    cloud_storage_bucket: Optional[str],
    primary_keys: List[str],
    task_name_format: str,
    task_queue_id__name: str,
    task_queue_id: Any,
    max_attempts: int,
    attempt_poll_s: int,
) -> Tuple[Task, pd.DataFrame]:
    """
    Creates a new task in CVAT or returns an existing one, and also associates it with images.

    If preannotations are provided, they are uploaded in XML format (`CVAT 1.1`).

    :param df__batch: Subset of rows belonging to one `inner_task_id`.
    :param file_path_column: Name of the column with the file path.
    :param cvat_client: Initialized CVAT client.
    :param project_id: CVAT project ID.
    :param inner_task_id: Internal batch ID (Datapipe).
    :param cloud_storage_bucket: Name of the bucket from which CVAT reads images. If None, files are uploaded directly.
    :param primary_keys: List of primary keys (for join and merge).

    :return: Tuple of Task and DataFrame with metadata (frames, path, task_id, etc.).
    """
    ctx = {
        "inner_task_id": inner_task_id,
        "date": datetime.now(timezone.utc),
        **{task_queue_id__name: task_queue_id},
    }
    new_task_name = task_name_format.format(**ctx)
    lookup_regex = build_regex_from_format(task_name_format, inner_task_id, task_queue_id__name, task_queue_id)

    df__batch = df__batch.copy()
    if file_path_column in df__batch.columns and cloud_storage_bucket is None:
        df__batch["cvat__file_path"] = df__batch[file_path_column].apply(lambda filepath: Path(filepath).name)
    elif file_path_column in df__batch.columns:
        df__batch["cvat__file_path"] = df__batch[file_path_column].apply(extract_key)
    expected_frames = len(df__batch["cvat__file_path"])

    project = cvat_client.projects.retrieve(project_id)
    matching_tasks = [t for t in project.get_tasks() if lookup_regex.match(t.name)]

    task_already_exists = len(matching_tasks) > 0
    task: Task | None = matching_tasks[0] if task_already_exists else None
    new_task_created = False

    try:
        if not task_already_exists:
            logger.info(
                "Creating CVAT task '%s' with %d resources in project %d (scope=%s:%s)",
                new_task_name, expected_frames, project_id, task_queue_id__name, task_queue_id
            )
            for attempt in range(1, max_attempts + 1):
                try:
                    if cloud_storage_bucket is None:
                        resources = df__batch[file_path_column].tolist()
                        resource_type = ResourceType.LOCAL
                        data_params = {
                            "image_quality": 75,
                            "use_zip_chunks": True,
                            "use_cache": True,
                            "sorting_method": "random",
                        }
                    else:
                        resources = df__batch["cvat__file_path"].tolist()
                        resource_type = ResourceType.SHARE
                        data_params = {
                            "cloud_storage_id": get_cloud_storage(cvat_client, cloud_storage_bucket),
                            "image_quality": 75,
                            "use_zip_chunks": True,
                            "use_cache": True,
                            "sorting_method": "random",
                        }
                    task = cvat_client.tasks.create_from_data(
                        spec=TaskWriteRequest(
                            name=new_task_name,
                            project_id=project_id,
                        ),
                        resources=resources,
                        resource_type=resource_type,
                        data_params=data_params,
                    )
                    logger.info(f"create_from_data: attempt: {attempt}: success")
                    logger.info(f"create_from_data: task: {task}")
                    new_task_created = True
                    break
                except Exception as e:
                    logger.info(f"create_from_data: attempt: {attempt}: exception: {e}")
                    payload, task_id_payload = _parse_api_exception(e)
                    transient = _is_transient_failure(payload)

                    if task_id_payload:
                        try:
                            cvat_client.tasks.remove_by_ids([task_id_payload])
                        except Exception as cleanup_err:
                            logger.error("Cannot delete failed task %s from payload: %s", task_id_payload, cleanup_err)
                        new_task_created = False

                    if attempt == max_attempts or not transient:
                        raise
                    sleep_s = attempt_poll_s
                    logger.warning(
                        "Transient CVAT error on create/upload (attempt %d/%d): %s; retry in %ds",
                        attempt, max_attempts, getattr(e, "reason", repr(e)), sleep_s
                    )
                    time.sleep(sleep_s)

        if task is None:
            raise RuntimeError("CVAT task was not created or found")

        meta = task.get_meta()
        df__meta = pd.DataFrame(
            {
                "project_id": project_id,
                "inner_task_id": inner_task_id,
                task_queue_id__name: task_queue_id,
                "task_id": task.id,
                "cvat__file_path": frame["name"],
                "inner_frame_id": frame_id,
            }
            for frame_id, frame in enumerate(meta["frames"])
        )

        df__batch_with_meta = pd.merge(
            df__batch[primary_keys + ["cvat__file_path"]],
            df__meta,
            on=[task_queue_id__name, "cvat__file_path"],
        ).sort_values(by=[task_queue_id__name, "inner_frame_id"])

        annotations = task.get_annotations()
        if _annotations_are_empty(annotations) and "annotations" in df__batch.columns:
            labels_names = [lbl.name for lbl in task.get_labels()]

            df__batch_with_meta = pd.merge(
                df__batch_with_meta,
                df__batch[["cvat__file_path", "annotations"]],
                on="cvat__file_path",
            )

            root = ET.Element("annotations")
            ET.SubElement(root, "version").text = "1.1"

            for inner_frame_id, annotation, cvat_path in zip(
                df__batch_with_meta["inner_frame_id"],
                df__batch_with_meta["annotations"],
                df__batch_with_meta["cvat__file_path"],
            ):
                img_elem = ET.fromstring(annotation)

                for label in {e.get("label") for e in img_elem.iter() if e.get("label")}:
                    if label not in labels_names:
                        project.update({"labels": [PatchedLabelRequest(name=label)]})
                        labels_names.append(label)

                img_elem.set("id", str(inner_frame_id))
                img_elem.set("name", cvat_path)
                root.append(img_elem)

            ET.indent(root)

            with tempfile.TemporaryDirectory() as tmpdir:
                zip_path = Path(tmpdir) / "annotations.zip"
                with zipfile.ZipFile(zip_path, "w") as zf:
                    zf.writestr("annotations.xml", ET.tostring(root))
                _import_annotations_with_retry(
                    task=task,
                    filename=str(zip_path),
                    format_name="CVAT 1.1",
                    max_attempts=max_attempts,
                    attempt_poll_s=attempt_poll_s,
                )

        return task, df__batch_with_meta

    except Exception:
        if new_task_created:
            if task is None:
                raise
            for attempt in range(1, max_attempts + 1):
                try:
                    cvat_client.tasks.retrieve(task.id)
                    task.remove()
                    logger.info(f"remove_task: attempt: {attempt}: success")
                    break
                except NotFoundException:
                    break
                except Exception as cleanup_err:
                    logger.error("Can't rollback and delete task %s: %s", task.id, cleanup_err)
                    if attempt == max_attempts:
                        raise
                    time.sleep(attempt_poll_s)
                    continue
        raise


def update_cvat_task_status(
    ds: DataStore,
    input_dts: List[DataTable],
    output_dts: List[DataTable],
    run_config: Optional[RunConfig],
    kwargs: Optional[Dict[str, Any]] = None,
):
    """
    Updates the status of CVAT tasks in the sync table.

    For each task (`task_id`) from the input table:
    - Retrieves its first job;
    - Saves the status (`state`) and last update date (`updated_date`) in the output table.

    :param ds: DataStore object (not used explicitly).
    :param input_dts: Input DataTables (expected [cvat_task]).
    :param output_dts: Output DataTables (expected [cvat_task_sync_table]).
    :param run_config: Run configuration (not used).
    :param kwargs: Additional parameters, must contain `cvat_url`, `cvat_organization`, and `cvat_credentials`.
    """

    kwargs = kwargs or {}
    cvat_url: str = kwargs["cvat_url"]
    cvat_organization: str = kwargs["cvat_organization"]
    cvat_credentials: Tuple[str, str] = kwargs["cvat_credentials"]
    max_attempts: int = kwargs["max_attempts"]
    attempt_poll_s: int = kwargs["attempt_poll_s"]

    dt__cvat_task: DataTable = input_dts[0]
    dt__cvat_task_sync_table: DataTable = output_dts[0]

    df__cvat_task = dt__cvat_task.get_data()

    for idx in df__cvat_task.index:
        task_id = int(df__cvat_task.loc[idx, "task_id"])

        for attempt in range(1, max_attempts + 1):
            try:
                cvat_client: CVATClient = create_cvat_client(cvat_url, cvat_organization, cvat_credentials)
                task = cvat_client.tasks.retrieve(int(df__cvat_task.loc[idx, "task_id"]))
                job = task.get_jobs()[0]
                df__cvat_task.loc[idx, "cvat_job__status"] = job.state
                df__cvat_task.loc[idx, "cvat_job__last_updated"] = job.updated_date
                break
            except Exception as e:
                logger.error("Cannot retrieve status of task_id %d: %s", task_id, str(e))
                time.sleep(attempt_poll_s)
                continue

    dt__cvat_task_sync_table.store_chunk(df__cvat_task)


def fetch_annotations_from_cvat(
    df__cvat_files: pd.DataFrame,
    df__cvat_task_sync_table: pd.DataFrame,
    primary_keys: List[str],
    cvat_url: str,
    cvat_organization: str,
    cvat_credentials: Tuple[str, str],
    file_type: Literal["image", "video"],
    task_queue_id__name: str,
    max_attempts: int,
    attempt_poll_s: int,
) -> pd.DataFrame:
    """
    Collects annotations from completed CVAT tasks and merges them with image metadata.

    :param df__cvat_files: Table with frames/files and their CVAT attributes.
    :param df__cvat_task_sync_table: Table with task_id and job statuses.
    :param primary_keys: Primary keys for joining with the main table.
    :param cvat_url: CVAT URL.
    :param cvat_organization: CVAT organization slug.
    :param cvat_credentials: Tuple of (username, password).
    :param file_type: File type — 'image' or 'video'.
    :return: DataFrame with columns: primary_keys + ['annotations']
    """

    df__cvat_task_sync_table = df__cvat_task_sync_table[df__cvat_task_sync_table["cvat_job__status"] == "completed"]

    df__cvat_files = pd.merge(
        df__cvat_files,
        df__cvat_task_sync_table,
        on=["project_id", task_queue_id__name, "task_id", "inner_task_id"],
    )

    task_ids = list(set(df__cvat_task_sync_table["task_id"]))
    cvat_annotation_dfs = []

    for task_id in task_ids:
        for attempt in range(1, max_attempts + 1):
            try:
                cvat_client: CVATClient = create_cvat_client(cvat_url, cvat_organization, cvat_credentials)
                df__task_id_cvat_annotation = export_job_annotations(cvat_client, task_id, file_type)
                break
            except Exception as e:
                logger.error("Cannot retrieve annotations of task_id %d: %s", task_id, str(e))
                if attempt == max_attempts:
                    raise
                time.sleep(attempt_poll_s)
                continue
        df__task_id_cvat_annotation = pd.merge(
            df__cvat_files[primary_keys + ["inner_task_id", "cvat__file_path"]], df__task_id_cvat_annotation, on="cvat__file_path"
        )

        cvat_annotation_dfs.append(df__task_id_cvat_annotation)

    if len(cvat_annotation_dfs) > 0:
        df__cvat_annotation = pd.concat(cvat_annotation_dfs, ignore_index=True)
    else:
        df__cvat_annotation = pd.DataFrame(columns=primary_keys + ["inner_task_id", "annotations"])

    return df__cvat_annotation[primary_keys + ["inner_task_id", "annotations"]]


def upload_batches_to_cvat(
    df__input: pd.DataFrame,
    df__input_batches: pd.DataFrame,
    df__cvat_task: pd.DataFrame,
    df__cvat_files: pd.DataFrame,
    idx: IndexDF,
    primary_keys: List[str],
    input_batches_dt: DataTable,
    cvat_files_dt: DataTable,
    cvat_task_dt: DataTable,
    task_sync_table_dt: DataTable,
    cvat_url: str,
    cvat_organization: str,
    cvat_credentials: Tuple[str, str],
    delete_cvat_tasks: bool,
    file_path_column: str,
    cvat_project_id: int,
    cloud_storage_bucket: Optional[str],
    task_queue_id__name: str,
    task_name_format: str,
    max_attempts: int,
    attempt_poll_s: int,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Uploads new files (images or videos) to CVAT, grouping them by `inner_task_id`.

    Creates CVAT tasks if they do not yet exist and associates them with corresponding files.
    Returns updated tables `df__cvat_task` and `df__cvat_files`.

    :param df__input: Original file rows (including path).
    :param df__input_batches: Same rows but already with `inner_task_id`.
    :param df__cvat_task: Table with already created CVAT tasks.
    :param df__cvat_files: Table with files already uploaded to CVAT.
    :param idx: IndexDF for processing.

    :return: Tuple of two dataframes:
            - df__cvat_task: updated task table
            - df__cvat_files: files table with links to tasks and frames
    """

    if delete_cvat_tasks:
        df_existing_input_batches_to_be_deleted = input_batches_dt.get_data(idx=idx)
        df_existing_tasks_to_be_deleted = df_existing_input_batches_to_be_deleted.merge(
            df__cvat_files, on=primary_keys + ["inner_task_id"]
        )

        if len(df_existing_tasks_to_be_deleted) > 0:
            cvat_client: CVATClient = create_cvat_client(cvat_url, cvat_organization, cvat_credentials)
            cvat_client.tasks.remove_by_ids(task_ids=list(set(df_existing_tasks_to_be_deleted["task_id"])))

            input_batches_dt.delete_by_idx(
                idx=data_to_index(
                    df_existing_tasks_to_be_deleted,
                    primary_keys + ["inner_task_id"],
                )
            )
            cvat_files_dt.delete_by_idx(
                idx=data_to_index(
                    df_existing_tasks_to_be_deleted,
                    primary_keys + ["project_id", "inner_task_id", "task_id"],
                )
            )
            cvat_task_dt.delete_by_idx(
                idx=data_to_index(
                    df_existing_tasks_to_be_deleted,
                    ["project_id", "inner_task_id", "task_id"],
                )
            )
            task_sync_table_dt.delete_by_idx(
                idx=data_to_index(
                    df_existing_tasks_to_be_deleted,
                    ["project_id", "inner_task_id", "task_id"],
                )
            )
            df__cvat_task = df__cvat_task[~df__cvat_task["task_id"].isin(df_existing_tasks_to_be_deleted["task_id"])]
            df__cvat_files = df__cvat_files[~df__cvat_files["task_id"].isin(df_existing_tasks_to_be_deleted["task_id"])]

    df__input_batches = pd.merge(df__input, df__input_batches)
    df__files = (df__input_batches if len(df__input_batches) >= len(df__cvat_files) else df__cvat_files).copy()

    new_dfs__cvat_files: List[pd.DataFrame] = []
    new_tasks_records: List[dict] = []

    for (task_queue_id, inner_task_id), df__batch in df__files.groupby([task_queue_id__name, "inner_task_id"]):
        inner_task_id_scalar: Hashable = inner_task_id
        for attempt in range(1, max_attempts + 1):
            try:
                cvat_client = create_cvat_client(cvat_url, cvat_organization, cvat_credentials)
                task, df__batch_with_meta = get_or_create_task(
                    df__batch=df__batch,
                    file_path_column=file_path_column,
                    cvat_client=cvat_client,
                    project_id=cvat_project_id,
                    inner_task_id=int_from_scalar(inner_task_id_scalar),
                    cloud_storage_bucket=cloud_storage_bucket,
                    primary_keys=primary_keys,
                    task_name_format=task_name_format,
                    task_queue_id=task_queue_id,
                    task_queue_id__name=task_queue_id__name,
                    max_attempts=max_attempts,
                    attempt_poll_s=attempt_poll_s,
                )

                new_tasks_records.append(
                    {
                        "project_id": cvat_project_id,
                        task_queue_id__name: task_queue_id,
                        "inner_task_id": inner_task_id,
                        "task_id": task.id,
                    }
                )
                new_dfs__cvat_files.append(df__batch_with_meta)

                logger.info(
                    "Uploaded %d new files to CVAT: task_id = %d (inner_task_id = %d)",
                    len(df__batch),
                    task.id,
                    inner_task_id,
                )
                break
            except Exception as e:
                logger.info(f"upload_batches_to_cvat: attempt: {attempt}: exception: {e}")
                raise
                if attempt == max_attempts:
                    raise
                time.sleep(attempt_poll_s)
                continue

    df__cvat_task_new = pd.DataFrame(
        new_tasks_records, columns=["project_id", task_queue_id__name, "task_id", "inner_task_id"]
    )
    if len(new_dfs__cvat_files) > 0:
        df__cvat_files_new = pd.concat(new_dfs__cvat_files, ignore_index=True)
    else:
        df__cvat_files_new = pd.DataFrame(columns=primary_keys + ["project_id", "inner_task_id", "task_id", "cvat__file_path", "inner_frame_id"])

    return (
        df__cvat_task_new[["project_id", task_queue_id__name, "task_id", "inner_task_id"]],
        df__cvat_files_new[primary_keys + ["project_id", "inner_task_id", "task_id", "cvat__file_path", "inner_frame_id"]],
    )


@dataclass
class CVATStep(PipelineStep):
    """
    Special step class for Datapipe integration with CVAT.
    """

    input: PipelineInput  # Input table with data.

    output__input_batches: PipelineOutput  # Output table with input data batches.
    output__cvat_task: PipelineOutput  # Output table with CVAT task data and Datapipe.
    output__cvat_files: PipelineOutput  # Output table with CVAT file data and Datapipe.
    output__cvat_annotation: PipelineOutput  # Output table with CVAT task annotations.

    task_sync_table: str  # Name of the table syncing CVAT tasks with Datapipe.

    # --- CVAT connection ----------------------------------------------------
    cvat_url: str  # URL to CVAT.
    cvat_organization: str  # Organization in CVAT.
    cvat_credentials: Tuple[str, str]  # (username, password)
    cvat_project_id: int

    # --- step behaviour -----------------------------------------------------
    primary_keys: List[str]
    file_path_column: str
    cloud_storage_bucket: Optional[str]
    delete_cvat_tasks: bool = False
    file_type: Literal["image", "video"] = "image"
    files_batch: Union[int, dict[Any, int]] = 100
    minimum_files_in_job: Union[int, dict[Any, int]] = 50
    task_queue_id__name: str = "task_queue_id"
    task_name_format: str = "[{date:%Y-%m-%d}] {task_queue_id} batch={inner_task_id}"
    sampling_order: Literal["default", "random"] = "default"
    sampling_random_seed: Optional[int] = None
    max_attempts: int = 5
    attempt_poll_s: int = 60

    create_table: bool = False
    labels: Optional[Labels] = None

    def __post_init__(self):
        """
        Post-initialization processing.
        """

        assert (
            "image__path" not in self.primary_keys
        ), "`image__path` should not be part of primary_keys – it is implicitly unique."
        self.labels = self.labels or []

    def build_compute(self, ds: DataStore, catalog: Catalog) -> List[ComputeStep]:
        """
        Method to build ComputeSteps for Datapipe.

        :param ds: Datapipe DataStore.
        :param catalog: Datapipe Catalog.
        """
        assert (
            f"{{{self.task_queue_id__name}}}" in self.task_name_format
        ), f"task_name_format must include placeholder {{{self.task_queue_id__name}}}"
        assert (
            self.task_queue_id__name in self.primary_keys
        ), f"task_queue_id__name='{self.task_queue_id__name}' must be in primary_keys"
        placeholders = [f"{{{self.task_queue_id__name}}}", "{inner_task_id}"]

        for placeholder in placeholders:
            if placeholder not in self.task_name_format:
                raise ValueError(f"task_name_format must include placeholder {{{placeholder}}}")

        if isinstance(self.files_batch, int) and isinstance(self.minimum_files_in_job, int):
            if self.files_batch < self.minimum_files_in_job:
                raise ValueError(
                    f"files_batch ({self.files_batch}) must be >= minimum_files_in_job ({self.minimum_files_in_job})"
                )
        elif isinstance(self.files_batch, dict) and isinstance(self.minimum_files_in_job, dict):
            if sorted(self.files_batch.keys()) != sorted(self.minimum_files_in_job.keys()):
                raise ValueError(
                    "files_batch and minimum_files_in_job must have the same keys"
                )
            for key, value in self.minimum_files_in_job.items():
                if self.files_batch[key] < self.minimum_files_in_job[key]:
                    raise ValueError(
                        f"files_batch ({self.files_batch[key]}) must be >= minimum_files_in_job ({value})"
                    )
        elif isinstance(self.files_batch, int) and isinstance(self.minimum_files_in_job, dict):
            for key, value in self.minimum_files_in_job.items():
                if self.files_batch < value:
                    raise ValueError(
                        f"files_batch ({self.files_batch}) must be >= minimum_files_in_job ({value})"
                    )
        elif isinstance(self.files_batch, dict) and isinstance(self.minimum_files_in_job, int):
            for key, value in self.files_batch.items():
                if value < self.minimum_files_in_job:
                    raise ValueError(
                        f"files_batch ({value}) must be >= minimum_files_in_job ({self.minimum_files_in_job})"
                    )

        input_name = get_pipeline_input_name(self.input)
        output_input_batches_name = get_pipeline_output_name(self.output__input_batches)
        output_cvat_task_name = get_pipeline_output_name(self.output__cvat_task)
        output_cvat_files_name = get_pipeline_output_name(self.output__cvat_files)
        output_cvat_annotation_name = get_pipeline_output_name(self.output__cvat_annotation)

        dt_input = ds.get_table(input_name)

        assert isinstance(dt_input.table_store, TableStoreDB)

        for col in self.primary_keys + [self.file_path_column]:
            if col not in [c.name for c in dt_input.table_store.get_schema()]:
                raise ValueError(f"Column {col} not found in {input_name}")

        if "cvat__file_path" in [c.name for c in dt_input.table_store.get_schema()]:
            raise ValueError("Column `cvat__file_path` is reserved for internal use")

        def _mk(dt_name: str, schema: List[Column]):
            dt = ds.get_or_create_table(
                dt_name,
                TableStoreDB(
                    dbconn=ds.meta_dbconn,
                    name=dt_name,
                    data_sql_schema=schema,
                    create_table=self.create_table,
                ),
            )
            catalog.add_datatable(dt_name, Table(dt.table_store))
            return dt

        cvat_task_dt = _mk(
            output_cvat_task_name,
            [
                Column("project_id", Integer, primary_key=True),
                Column(self.task_queue_id__name, String, primary_key=True),
                Column("inner_task_id", Integer, primary_key=True),
                Column("task_id", Integer, primary_key=True),
            ],
        )

        task_sync_table_dt = _mk(
            self.task_sync_table,
            [
                Column("project_id", Integer, primary_key=True),
                Column(self.task_queue_id__name, String, primary_key=True),
                Column("inner_task_id", Integer, primary_key=True),
                Column("task_id", Integer, primary_key=True),
                Column("cvat_job__status", String),
                Column("cvat_job__last_updated", DateTime),
            ],
        )

        input_batches_dt = _mk(
            output_input_batches_name,
            [column for column in dt_input.table_store.get_schema() if column.name in self.primary_keys]
            + [Column("inner_task_id", Integer, primary_key=True)],
        )

        cvat_files_dt = _mk(
            output_cvat_files_name,
            [column for column in dt_input.table_store.get_schema() if column.name in self.primary_keys]
            + [
                Column("project_id", Integer, primary_key=True),
                Column("inner_task_id", Integer, primary_key=True),
                Column("task_id", Integer, primary_key=True),
                Column("cvat__file_path", String),
                Column("inner_frame_id", Integer),
            ],
        )

        _mk(
            output_cvat_annotation_name,
            [column for column in dt_input.table_store.get_schema() if column.name in self.primary_keys]
            + [Column("inner_task_id", Integer, primary_key=True)]
            + [Column("annotations", String)],
        )

        pipeline = Pipeline(
            [
                BatchTransform(
                    func=assign_batches_to_files,
                    inputs=[self.input, output_input_batches_name],
                    outputs=[self.output__input_batches],
                    chunk_size=1,
                    kwargs=dict(
                        primary_keys=self.primary_keys,
                        files_batch=self.files_batch,
                        minimum_files_in_job=self.minimum_files_in_job,
                        output__input_batches=output_input_batches_name,
                        task_queue_id__name=self.task_queue_id__name,
                        sampling_order=self.sampling_order,
                        sampling_random_seed=self.sampling_random_seed,
                    ),
                    transform_keys=[self.task_queue_id__name],
                    labels=self.labels,
                    order_by=[self.task_queue_id__name],
                    executor_config=ExecutorConfig(parallelism=0),
                ),
                BatchTransform(
                    func=upload_batches_to_cvat,
                    inputs=[
                        self.input,
                        output_input_batches_name,
                        output_cvat_task_name,
                        output_cvat_files_name,
                    ],
                    outputs=[self.output__cvat_task, self.output__cvat_files],
                    chunk_size=1,
                    labels=self.labels,
                    transform_keys=[self.task_queue_id__name, "inner_task_id"],
                    order_by=["inner_task_id", self.task_queue_id__name],
                    kwargs=dict(
                        primary_keys=self.primary_keys,
                        input_batches_dt=input_batches_dt,
                        cvat_files_dt=cvat_files_dt,
                        cvat_task_dt=cvat_task_dt,
                        task_sync_table_dt=task_sync_table_dt,
                        cvat_url=self.cvat_url,
                        cvat_organization=self.cvat_organization,
                        cvat_credentials=self.cvat_credentials,
                        delete_cvat_tasks=self.delete_cvat_tasks,
                        file_path_column=self.file_path_column,
                        cvat_project_id=self.cvat_project_id,
                        cloud_storage_bucket=self.cloud_storage_bucket,
                        task_name_format=self.task_name_format,
                        task_queue_id__name=self.task_queue_id__name,
                        max_attempts=self.max_attempts,
                        attempt_poll_s=self.attempt_poll_s,
                    ),
                    executor_config=ExecutorConfig(parallelism=0),
                ),
                DatatableTransform(
                    func=update_cvat_task_status,  # type: ignore
                    inputs=[output_cvat_task_name],
                    outputs=[self.task_sync_table],
                    check_for_changes=False,
                    labels=self.labels,
                    kwargs=dict(
                        cvat_url=self.cvat_url,
                        cvat_organization=self.cvat_organization,
                        cvat_credentials=self.cvat_credentials,
                        max_attempts=self.max_attempts,
                        attempt_poll_s=self.attempt_poll_s,
                    ),
                ),
                BatchTransform(
                    func=fetch_annotations_from_cvat,
                    inputs=[output_cvat_files_name, self.task_sync_table],
                    outputs=[self.output__cvat_annotation],
                    labels=self.labels,
                    transform_keys=[self.task_queue_id__name, "inner_task_id"],
                    kwargs=dict(
                        primary_keys=self.primary_keys,
                        cvat_url=self.cvat_url,
                        cvat_organization=self.cvat_organization,
                        cvat_credentials=self.cvat_credentials,
                        file_type=self.file_type,
                        task_queue_id__name=self.task_queue_id__name,
                        max_attempts=self.max_attempts,
                        attempt_poll_s=self.attempt_poll_s,
                    ),
                    executor_config=ExecutorConfig(parallelism=0),
                    chunk_size=1,
                ),
            ]
        )
        return build_compute(ds, catalog, pipeline)
