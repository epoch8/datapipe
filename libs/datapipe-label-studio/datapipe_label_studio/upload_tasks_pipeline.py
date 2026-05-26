import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple, Union, cast

import numpy as np
import pandas as pd
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
    index_to_data,
)
from label_studio_sdk import LabelStudio
from label_studio_sdk.data_manager import Column as ColumnLS
from label_studio_sdk.data_manager import Filters, Operator, Type
from label_studio_sdk.types.import_api_request import (
    ImportApiRequest as LSImportApiRequest,
)
from sqlalchemy import JSON, Column, DateTime, Integer

from datapipe_label_studio.sdk_utils import (
    ensure_project,
    ensure_project_storages,
    get_ls_client,
    get_tasks_iter,
    import_tasks_response_to_dict,
)
from datapipe_label_studio.types import GCSBucket, S3Bucket
from datapipe_label_studio.utils import check_columns_are_in_table

logger = logging.getLogger("dataipipe_label_studio_lite")


def convert_data_if_need(value: Any):
    if isinstance(value, np.int64):
        return int(value)
    return value


def _to_utc_aware(value: Any) -> datetime:
    ts = pd.Timestamp(value).to_pydatetime()
    if ts.tzinfo is None:
        return ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


def _to_utc_naive(value: Any) -> datetime:
    ts = pd.Timestamp(value).to_pydatetime()
    if ts.tzinfo is not None:
        ts = ts.astimezone(timezone.utc)
    return ts.replace(tzinfo=None)


def delete_task_from_project(client: LabelStudio, task_id: Any) -> None:
    try:
        client.tasks.delete(id=str(task_id))
    except Exception as exc:
        status_code = getattr(exc, "status_code", None)
        if status_code not in [404, 500]:
            raise


# created_ago - очень плохой параметр, он меняется каждый раз, когда происходит запрос
def _cleanup(values):
    for ann in values:
        if "created_ago" in ann:
            del ann["created_ago"]
    return values


def upload_tasks_to_label_studio(
    df: pd.DataFrame,
    idx: IndexDF,
    get_project_context: Callable[[], Tuple[LabelStudio, int]],
    primary_keys: List[str],
    columns: List[str],
    delete_unannotated_tasks_only_on_update: bool,
    dt__output__label_studio_project_task: DataTable,
    dt__output__label_studio_project_annotation: DataTable,
) -> pd.DataFrame:
    """
    Добавляет в LS новые задачи с заданными ключами.
    """
    if df.empty and idx.empty:
        return pd.DataFrame(columns=primary_keys + ["task_id"])

    client, project_id = get_project_context()
    idx = data_to_index(idx, primary_keys)
    if delete_unannotated_tasks_only_on_update:
        df_idx = data_to_index(df, primary_keys)
        df_existing_tasks = dt__output__label_studio_project_task.get_data(idx=idx)
        filters = Filters.create(
            conjunction="or",
            items=[
                Filters.item(
                    name=ColumnLS.id,
                    operator=Operator.EQUAL,
                    column_type=Type.Number,
                    value=Filters.value(task_id),
                )
                for task_id in df_existing_tasks["task_id"]
            ],
        )
        tasks = []
        for tasks_page in get_tasks_iter(client, project_id, filters=filters):
            tasks.extend(tasks_page)
        df__output__label_studio_project_annotation = (
            pd.DataFrame.from_records(
                {
                    **{column: [task["data"].get(column) for task in tasks] for column in primary_keys + columns},
                    "annotations": [_cleanup(task["annotations"]) for task in tasks],
                    "task_id": [task["id"] for task in tasks],
                }
            )
            .sort_values(by="task_id", ascending=False)
            .drop_duplicates(subset=primary_keys)
            .drop(columns=["task_id"])
        )
        if len(df__output__label_studio_project_annotation) == 0:
            df_existing_tasks_with_output = df_existing_tasks.copy()
            df_existing_tasks_with_output["annotations"] = df_existing_tasks_with_output.apply(lambda row: [], axis=1)
        else:
            df_existing_tasks_with_output = pd.merge(
                df_existing_tasks,
                df__output__label_studio_project_annotation,
                how="left",
                on=primary_keys,
            ).drop(columns=[x for x in columns if x not in primary_keys])
        deleted_idx = index_difference(df_idx, idx)
        if len(df_existing_tasks_with_output) > 0:
            have_annotations = df_existing_tasks_with_output["annotations"].apply(
                lambda ann: isinstance(ann, list) and len(ann) > 0 and bool(pd.notna(ann).any())
            )
            df_existing_tasks_to_be_stayed = df_existing_tasks_with_output[have_annotations]
            df_existing_tasks_to_be_deleted = pd.merge(
                df_existing_tasks_with_output[~have_annotations], deleted_idx, how="outer", on=primary_keys
            )
        else:
            df_existing_tasks_to_be_stayed = pd.DataFrame(columns=primary_keys + ["task_id"])
            df_existing_tasks_to_be_deleted = pd.merge(
                pd.DataFrame(columns=primary_keys + ["task_id"]), deleted_idx, how="outer", on=primary_keys
            )
        df_to_be_uploaded = pd.concat(
            [
                pd.merge(df, df_existing_tasks_to_be_deleted, on=primary_keys),
                index_to_data(
                    df,
                    index_difference(
                        index_difference(
                            df_idx,
                            data_to_index(
                                df_existing_tasks_to_be_stayed,
                                primary_keys,
                            ),
                        ),
                        data_to_index(df_existing_tasks_to_be_deleted, primary_keys),
                    ),
                ),
            ],
            ignore_index=True,
        )
    else:
        df_existing_tasks_to_be_deleted = dt__output__label_studio_project_task.get_data(idx=idx)
        df_to_be_uploaded = df

    if len(df_existing_tasks_to_be_deleted) > 0:
        for task_id in df_existing_tasks_to_be_deleted["task_id"]:
            delete_task_from_project(client, task_id)
        dt__output__label_studio_project_annotation.delete_by_idx(
            idx=data_to_index(
                dt__output__label_studio_project_annotation.get_data(
                    idx=data_to_index(df_existing_tasks_to_be_deleted, primary_keys)
                ),
                primary_keys,
            )
        )

    if df.empty and not delete_unannotated_tasks_only_on_update:
        return pd.DataFrame(columns=primary_keys + ["task_id"])

    if len(df_to_be_uploaded) > 0:
        data_to_be_added: List[LSImportApiRequest] = [
            cast(
                LSImportApiRequest,
                {
                    "data": {
                        column: convert_data_if_need(df_to_be_uploaded.loc[idx, column])
                        for column in primary_keys + columns
                    }
                },
            )
            for idx in df_to_be_uploaded.index
        ]
        import_response = client.projects.import_tasks(
            id=project_id,
            request=data_to_be_added,
            return_task_ids=True,
        )
        response_data = import_tasks_response_to_dict(import_response)
        task_ids = response_data.get("task_ids")
        if not task_ids:
            raise ValueError("Label Studio did not return task IDs for import_tasks.")
        df_to_be_uploaded["task_id"] = task_ids

    if delete_unannotated_tasks_only_on_update:
        df_res = pd.concat(
            [df_existing_tasks_to_be_stayed, df_to_be_uploaded],
            ignore_index=True,
        )
    else:
        df_res = df_to_be_uploaded
    logger.debug(f"Deleted {len(df_existing_tasks_to_be_deleted)} tasks, uploaded {len(df_to_be_uploaded)} tasks.")
    return df_res[primary_keys + ["task_id"]]


def get_annotations_from_label_studio(
    ds: DataStore,
    input_dts: List[DataTable],
    output_dts: List[DataTable],
    run_config: Optional[RunConfig],
    kwargs: Optional[Dict[str, Any]] = None,
):
    """
    Записывает в табличку задачи из сервера LS вместе с разметкой согласно
    дате последней синхронизации
    """
    kwargs = kwargs or {}
    get_project_context: Callable[[], Tuple[LabelStudio, int]] = kwargs["get_project_context"]
    client, project_id = get_project_context()
    primary_keys: List[str] = kwargs["primary_keys"]

    (dt__output__label_studio_sync_table, dt__output__label_studio_project_annotation) = output_dts
    dt__output__label_studio_project_task: DataTable = kwargs["dt__output__label_studio_project_task"]

    df__output__label_studio_sync_table = dt__output__label_studio_sync_table.get_data(
        idx=cast(IndexDF, pd.DataFrame({"project_id": [project_id]}))
    )

    if df__output__label_studio_sync_table.empty:
        df__output__label_studio_sync_table.loc[0, "project_id"] = project_id
        df__output__label_studio_sync_table.loc[0, "last_updated_at"] = datetime.fromtimestamp(0, tz=timezone.utc)

    last_sync = _to_utc_aware(df__output__label_studio_sync_table.loc[0, "last_updated_at"])
    filters = Filters.create(
        conjunction="and",
        items=[
            Filters.item(
                name=ColumnLS.updated_at,
                operator=Operator.GREATER,
                column_type=Type.Datetime,
                value=Filters.value(value=Filters.datetime(last_sync)),
            )
        ],
    )
    updated_ats = []
    for tasks_page in get_tasks_iter(client, project_id, filters=filters):
        updated_ats.extend([task["updated_at"] for task in tasks_page])
        output_df = pd.DataFrame.from_records(
            {
                **{primary_key: [task["data"][primary_key] for task in tasks_page] for primary_key in primary_keys},
                "annotations": [_cleanup(task["annotations"]) for task in tasks_page],
                "task_id": [task["id"] for task in tasks_page],
            }
        ).sort_values(by="task_id", ascending=False)
        df__output__label_studio_project_task = dt__output__label_studio_project_task.get_data(
            idx=cast(IndexDF, output_df)
        )
        if len(df__output__label_studio_project_task) > 0:
            output_df = pd.merge(df__output__label_studio_project_task, output_df).drop(columns=["task_id"])
            dt__output__label_studio_project_annotation.store_chunk(output_df)
    if len(updated_ats) > 0:
        df__output__label_studio_sync_table.loc[0, "last_updated_at"] = _to_utc_naive(max(updated_ats))
        dt__output__label_studio_sync_table.store_chunk(df__output__label_studio_sync_table)


@dataclass
class LabelStudioUploadTasks(PipelineStep):
    input__item: PipelineInput
    output__label_studio_project_task: PipelineOutput
    output__label_studio_project_annotation: PipelineOutput
    output__label_studio_sync_table: PipelineOutput

    ls_url: str
    api_key: str
    project_identifier: Union[str, int]  # project_title or id
    primary_keys: List[str]
    columns: List[str]

    chunk_size: int = 100
    project_label_config_at_create: str = ""
    project_description_at_create: str = ""
    storages: Optional[List[Union[GCSBucket, S3Bucket]]] = None
    create_table: bool = False
    delete_unannotated_tasks_only_on_update: bool = False
    labels: Optional[Labels] = None
    executor_config: Optional[ExecutorConfig] = None

    def __post_init__(self):
        for column in ["task_id", "annotations"]:
            assert column not in self.primary_keys, f'The column "{column}" is reserved for this PipelineStep.'
        if isinstance(self.project_identifier, str):
            assert len(self.project_identifier) <= 50

        # lazy initialization
        self._ls_client: Optional[LabelStudio] = None
        self._project_id: Optional[int] = None

        self.labels = self.labels or []
        self.storages = self.storages or []

    @property
    def ls_client(self) -> LabelStudio:
        if self._ls_client is None:
            self._ls_client = get_ls_client(self.ls_url, self.api_key)
        return self._ls_client

    def get_project_context(self) -> Tuple[LabelStudio, int]:
        """
        При первом использовании ищет проект в LS по индентификатору,
        если его нет -- автоматически создаётся проект с нуля.
        """
        if self._project_id is not None:
            return (self.ls_client, self._project_id)
        project = ensure_project(
            self.ls_client,
            self.project_identifier,
            self.project_label_config_at_create,
            self.project_description_at_create,
        )
        self._project_id = project["id"]
        ensure_project_storages(
            self.ls_client,
            self._project_id,
            self.storages,
        )
        return (self.ls_client, self._project_id)

    def build_compute(self, ds: DataStore, catalog: Catalog) -> List[ComputeStep]:
        input_item_name = get_pipeline_input_name(self.input__item)
        output_label_studio_project_task_name = get_pipeline_output_name(self.output__label_studio_project_task)
        output_label_studio_project_annotation_name = get_pipeline_output_name(
            self.output__label_studio_project_annotation
        )
        output_label_studio_sync_table_name = get_pipeline_output_name(self.output__label_studio_sync_table)

        dt__input_item = ds.get_table(input_item_name)
        assert isinstance(dt__input_item.table_store, TableStoreDB)
        check_columns_are_in_table(ds, self.input__item, self.primary_keys + self.columns)
        dt__output__label_studio_project_task = ds.get_or_create_table(
            output_label_studio_project_task_name,
            TableStoreDB(
                dbconn=ds.meta_dbconn,
                name=output_label_studio_project_task_name,
                data_sql_schema=[column for column in dt__input_item.table_store.get_primary_schema()]
                + [Column("task_id", Integer)],
                create_table=self.create_table,
            ),
        )
        catalog.add_datatable(
            output_label_studio_project_task_name, Table(dt__output__label_studio_project_task.table_store)
        )
        dt__output__label_studio_sync_table = ds.get_or_create_table(
            output_label_studio_sync_table_name,
            TableStoreDB(
                dbconn=ds.meta_dbconn,
                name=output_label_studio_sync_table_name,
                data_sql_schema=[
                    Column("project_id", Integer, primary_key=True),
                    Column("last_updated_at", DateTime),
                ],
                create_table=self.create_table,
            ),
        )
        catalog.add_datatable(
            output_label_studio_sync_table_name, Table(dt__output__label_studio_sync_table.table_store)
        )
        dt__output__label_studio_project_annotation = ds.get_or_create_table(
            output_label_studio_project_annotation_name,
            TableStoreDB(
                dbconn=ds.meta_dbconn,
                name=output_label_studio_project_annotation_name,
                data_sql_schema=[
                    column
                    for column in dt__input_item.table_store.get_schema()
                    if column.name in dt__input_item.table_store.primary_keys
                ]
                + [Column("annotations", JSON)],
                create_table=self.create_table,
            ),
        )
        catalog.add_datatable(
            output_label_studio_project_annotation_name, Table(dt__output__label_studio_project_annotation.table_store)
        )

        pipeline = Pipeline(
            [
                BatchTransform(
                    labels=self.labels,
                    func=upload_tasks_to_label_studio,
                    inputs=[self.input__item],
                    outputs=[self.output__label_studio_project_task],
                    chunk_size=self.chunk_size,
                    executor_config=self.executor_config,
                    kwargs=dict(
                        get_project_context=self.get_project_context,
                        primary_keys=self.primary_keys,
                        columns=self.columns,
                        delete_unannotated_tasks_only_on_update=self.delete_unannotated_tasks_only_on_update,
                        dt__output__label_studio_project_task=dt__output__label_studio_project_task,
                        dt__output__label_studio_project_annotation=dt__output__label_studio_project_annotation,
                    ),
                ),
                DatatableTransform(
                    labels=self.labels,
                    func=get_annotations_from_label_studio,
                    inputs=[],
                    outputs=[output_label_studio_sync_table_name, output_label_studio_project_annotation_name],
                    check_for_changes=False,
                    kwargs=dict(
                        get_project_context=self.get_project_context,
                        primary_keys=self.primary_keys,
                        dt__output__label_studio_project_task=dt__output__label_studio_project_task,
                    ),
                ),
            ]
        )
        return build_compute(ds, catalog, pipeline)


def upload_tasks_to_label_studio_projects(
    df__item: pd.DataFrame,
    df__label_studio_project: pd.DataFrame,
    idx: IndexDF,
    ls_client: LabelStudio,
    primary_keys: List[str],
    columns: List[str],
    delete_unannotated_tasks_only_on_update: bool,
    dt__output__label_studio_project_task: DataTable,
    dt__output__label_studio_project_annotation: DataTable,
) -> pd.DataFrame:
    project_identifiers = (
        set(df__item["project_identifier"])
        .union(set(df__label_studio_project["project_identifier"]))
        .union(set(idx["project_identifier"]))
    )
    dfs = []
    for project_identifier in project_identifiers:
        df_by_project_identifier = df__item[df__item["project_identifier"] == project_identifier]
        idx_by_project_identifier = idx[idx["project_identifier"] == project_identifier]
        if project_identifier not in set(df__label_studio_project["project_identifier"]):
            logger.info(f"Project {project_identifier} not found in input__label_studio_project. Skipping")
            continue
        project_id = int(
            df__label_studio_project[df__label_studio_project["project_identifier"] == project_identifier].iloc[0][
                "project_id"
            ]
        )
        logger.info(f"Uploading tasks to {project_identifier=} ({project_id=})")

        def _get_project_context(project_id: int = project_id) -> Tuple[LabelStudio, int]:
            return (ls_client, project_id)

        df__res = upload_tasks_to_label_studio(
            df=df_by_project_identifier,
            idx=idx_by_project_identifier,
            get_project_context=_get_project_context,
            primary_keys=primary_keys,
            columns=columns,
            delete_unannotated_tasks_only_on_update=delete_unannotated_tasks_only_on_update,
            dt__output__label_studio_project_task=dt__output__label_studio_project_task,
            dt__output__label_studio_project_annotation=dt__output__label_studio_project_annotation,
        )
        dfs.append(df__res)
    if len(dfs) == 0:
        dfs_res = pd.DataFrame(columns=primary_keys + ["task_id"])
    else:
        dfs_res = pd.concat(dfs, ignore_index=True)
    return dfs_res


def get_annotations_from_label_studio_projects(
    ds: DataStore,
    input_dts: List[DataTable],
    output_dts: List[DataTable],
    run_config: Optional[RunConfig],
    kwargs: Optional[Dict[str, Any]] = None,
) -> None:
    kwargs = kwargs or {}
    ls_client: LabelStudio = kwargs["ls_client"]
    dt__label_studio_project: DataTable = input_dts[0]
    df__label_studio_project = dt__label_studio_project.get_data()
    for project_identifier, project_id in zip(
        df__label_studio_project["project_identifier"], df__label_studio_project["project_id"]
    ):
        project_id = int(project_id)
        logger.info(f"Getting annotations from {project_identifier=} ({project_id=})")
        params_kwargs = kwargs.copy()
        params_kwargs["get_project_context"] = lambda project_id=project_id: (ls_client, project_id)
        get_annotations_from_label_studio(
            ds, input_dts=[], output_dts=output_dts, run_config=run_config, kwargs=params_kwargs
        )


@dataclass
class LabelStudioUploadTasksToProjects(PipelineStep):
    input__item: PipelineInput
    input__label_studio_project: PipelineInput
    output__label_studio_project_task: PipelineOutput
    output__label_studio_project_annotation: PipelineOutput
    output__label_studio_sync_table: PipelineOutput

    ls_url: str
    api_key: str
    primary_keys: List[str]
    columns: List[str]

    chunk_size: int = 100
    create_table: bool = False
    delete_unannotated_tasks_only_on_update: bool = False
    labels: Optional[Labels] = None
    executor_config: Optional[ExecutorConfig] = None

    def __post_init__(self):
        for column in ["task_id", "annotations"]:
            assert column not in self.primary_keys, f'The column "{column}" is reserved for this PipelineStep.'

        # lazy initialization
        self._ls_client: Optional[LabelStudio] = None

        self.labels = self.labels or []

    @property
    def ls_client(self) -> LabelStudio:
        if self._ls_client is None:
            self._ls_client = get_ls_client(self.ls_url, self.api_key)
        return self._ls_client

    def build_compute(self, ds: DataStore, catalog: Catalog) -> List[ComputeStep]:
        assert "project_identifier" in self.primary_keys
        input_item_name = get_pipeline_input_name(self.input__item)
        input_label_studio_project_name = get_pipeline_input_name(self.input__label_studio_project)
        output_label_studio_project_task_name = get_pipeline_output_name(self.output__label_studio_project_task)
        output_label_studio_project_annotation_name = get_pipeline_output_name(
            self.output__label_studio_project_annotation
        )
        output_label_studio_sync_table_name = get_pipeline_output_name(self.output__label_studio_sync_table)

        dt__input_item = ds.get_table(input_item_name)
        assert isinstance(dt__input_item.table_store, TableStoreDB)
        check_columns_are_in_table(ds, self.input__item, self.primary_keys)
        check_columns_are_in_table(ds, self.input__label_studio_project, ["project_identifier", "project_id"])
        dt__output__label_studio_project_task = ds.get_or_create_table(
            output_label_studio_project_task_name,
            TableStoreDB(
                dbconn=ds.meta_dbconn,
                name=output_label_studio_project_task_name,
                data_sql_schema=[
                    column
                    for column in dt__input_item.table_store.get_primary_schema()
                    if column.name in self.primary_keys
                ]
                + [Column("task_id", Integer)],
                create_table=self.create_table,
            ),
        )
        catalog.add_datatable(
            output_label_studio_project_task_name, Table(dt__output__label_studio_project_task.table_store)
        )
        dt__output__label_studio_sync_table = ds.get_or_create_table(
            output_label_studio_sync_table_name,
            TableStoreDB(
                dbconn=ds.meta_dbconn,
                name=output_label_studio_sync_table_name,
                data_sql_schema=[
                    Column("project_id", Integer, primary_key=True),
                    Column("last_updated_at", DateTime),
                ],
                create_table=self.create_table,
            ),
        )
        catalog.add_datatable(
            output_label_studio_sync_table_name, Table(dt__output__label_studio_sync_table.table_store)
        )
        dt__output__label_studio_project_annotation = ds.get_or_create_table(
            output_label_studio_project_annotation_name,
            TableStoreDB(
                dbconn=ds.meta_dbconn,
                name=output_label_studio_project_annotation_name,
                data_sql_schema=[
                    column
                    for column in dt__input_item.table_store.get_schema()
                    if column.name in self.primary_keys + self.columns
                ]
                + [Column("annotations", JSON)],
                create_table=self.create_table,
            ),
        )
        catalog.add_datatable(
            output_label_studio_project_annotation_name, Table(dt__output__label_studio_project_annotation.table_store)
        )

        pipeline = Pipeline(
            [
                BatchTransform(
                    labels=self.labels,
                    func=upload_tasks_to_label_studio_projects,
                    inputs=[self.input__item, self.input__label_studio_project],
                    outputs=[self.output__label_studio_project_task],
                    chunk_size=self.chunk_size,
                    executor_config=self.executor_config,
                    kwargs=dict(
                        ls_client=self.ls_client,
                        primary_keys=self.primary_keys,
                        columns=self.columns,
                        delete_unannotated_tasks_only_on_update=self.delete_unannotated_tasks_only_on_update,
                        dt__output__label_studio_project_task=dt__output__label_studio_project_task,
                        dt__output__label_studio_project_annotation=dt__output__label_studio_project_annotation,
                    ),
                    transform_keys=self.primary_keys,
                ),
                DatatableTransform(
                    labels=self.labels,
                    func=get_annotations_from_label_studio_projects,
                    inputs=[input_label_studio_project_name],
                    outputs=[output_label_studio_sync_table_name, output_label_studio_project_annotation_name],
                    check_for_changes=False,
                    kwargs=dict(
                        ls_client=self.ls_client,
                        primary_keys=self.primary_keys,
                        dt__output__label_studio_project_task=dt__output__label_studio_project_task,
                    ),
                ),
            ]
        )
        return build_compute(ds, catalog, pipeline)
