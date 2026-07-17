import json
from dataclasses import dataclass
from typing import Callable, List, Optional, Tuple, Union

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
from datapipe.step.batch_transform import BatchTransform
from datapipe.store.database import TableStoreDB
from datapipe.types import (
    IndexDF,
    Labels,
    PipelineInput,
    PipelineOutput,
    data_to_index,
    get_pipeline_input_name,
    get_pipeline_output_name,
)
from label_studio_sdk import LabelStudio
from sqlalchemy import JSON, Column, Integer, String

from datapipe_label_studio.sdk_utils import (
    get_ls_client,
    resolve_project_id,
)
from datapipe_label_studio.upload_tasks_pipeline import logger
from datapipe_label_studio.utils import check_columns_are_in_table


def _make_jsonable(value: object) -> object:
    return json.loads(json.dumps(value, default=str))


def upload_prediction_to_label_studio(
    df__item__has__prediction: pd.DataFrame,
    df__label_studio_project_task: pd.DataFrame,
    idx: IndexDF,
    get_project_context: Callable[[], Tuple[LabelStudio, int]],
    primary_keys: List[str],
    dt__output__label_studio_project_prediction: DataTable,
    model_version__column: str,
) -> pd.DataFrame:
    """
    Добавляет в LS предсказания.
    """
    df = pd.merge(df__item__has__prediction, df__label_studio_project_task, on=primary_keys)
    if (df__item__has__prediction.empty and df__label_studio_project_task.empty) and idx.empty:
        return pd.DataFrame(columns=primary_keys + ["task_id", "prediction"])

    client, project_id = get_project_context()
    idx = data_to_index(idx, primary_keys)
    df_existing_prediction_to_be_deleted = dt__output__label_studio_project_prediction.get_data(idx=idx)
    if len(df_existing_prediction_to_be_deleted) > 0:
        for prediction in df_existing_prediction_to_be_deleted["prediction"]:
            try:
                client.predictions.delete(id=prediction["id"])
            except Exception as exc:
                status_code = getattr(exc, "status_code", None)
                if status_code not in [404, 500]:
                    raise
        dt__output__label_studio_project_prediction.delete_by_idx(
            idx=data_to_index(df_existing_prediction_to_be_deleted, primary_keys)
        )

    if df.empty:
        return pd.DataFrame(columns=primary_keys + ["task_id"])

    df["model_version"] = df[model_version__column]
    # Не подходит из-за https://github.com/HumanSignal/label-studio/issues/4819
    # uploaded_predictions = self.project.create_predictions(
    #     [
    #         dict(
    #             task=row["task_id"],
    #             result=row["prediction"].get('result', []),
    #             model_version=row['model_version'],
    #             score=row["prediction"].get('score', 1.0)
    #         )
    #         for _, row in df.iterrows()
    #     ]
    # )
    uploaded_predictions = []
    for _, row in df.iterrows():
        prediction = client.predictions.create(
            task=row["task_id"],
            result=row["prediction"].get("result", []),
            model_version=row["model_version"],
            score=row["prediction"].get("score", 1.0),
        )
        uploaded_predictions.append(prediction)
    df["prediction_id"] = [getattr(prediction, "id", None) for prediction in uploaded_predictions]
    df["prediction"] = pd.Series(
        [
            _make_jsonable(
                prediction.model_dump() if hasattr(prediction, "model_dump") else prediction
            )
            for prediction in uploaded_predictions
        ],
        dtype=object,
        index=df.index,
    )
    return df[primary_keys + ["task_id", "prediction_id", "model_version", "prediction"]]


@dataclass
class LabelStudioUploadPredictions(PipelineStep):
    input__item__has__prediction: PipelineInput
    # prediction имеет такой вид: {"result": [...], "score": 0.}
    input__label_studio_project_task: PipelineInput
    output__label_studio_project_prediction: PipelineOutput

    ls_url: str
    api_key: str
    project_identifier: Union[str, int]  # project_title or id
    primary_keys: List[str]

    chunk_size: int = 100
    create_table: bool = False
    labels: Optional[Labels] = None
    model_version__column: str = "model_version"
    executor_config: Optional[ExecutorConfig] = None

    def __post_init__(self):
        if isinstance(self.project_identifier, str):
            assert len(self.project_identifier) <= 50

        # lazy initialization
        self._ls_client: Optional[LabelStudio] = None
        self._project_id: Optional[int] = None
        self.labels = self.labels or []

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
        self._project_id = resolve_project_id(self.ls_client, self.project_identifier)
        return (self.ls_client, self._project_id)

    def build_compute(self, ds: DataStore, catalog: Catalog) -> List[ComputeStep]:
        input_item_has_prediction_name = get_pipeline_input_name(self.input__item__has__prediction)
        output_label_studio_project_prediction_name = get_pipeline_output_name(
            self.output__label_studio_project_prediction
        )
        dt__input__has__prediction = ds.get_table(input_item_has_prediction_name)
        assert isinstance(dt__input__has__prediction.table_store, TableStoreDB)
        check_columns_are_in_table(
            ds, self.input__item__has__prediction, self.primary_keys + ["prediction", self.model_version__column]
        )
        check_columns_are_in_table(ds, self.input__label_studio_project_task, self.primary_keys + ["task_id"])
        catalog.add_datatable(
            output_label_studio_project_prediction_name,
            Table(
                ds.get_or_create_table(
                    output_label_studio_project_prediction_name,
                    TableStoreDB(
                        dbconn=ds.meta_dbconn,
                        name=output_label_studio_project_prediction_name,
                        data_sql_schema=[
                            column
                            for column in dt__input__has__prediction.primary_schema
                            if column.name in self.primary_keys
                        ]
                        + [
                            Column("task_id", Integer),
                            Column("prediction_id", Integer),
                            Column("model_version", String),
                            Column("prediction", JSON),
                        ],
                        create_table=self.create_table,
                    ),
                ).table_store
            ),
        )
        dt__output__label_studio_project_prediction = ds.get_table(output_label_studio_project_prediction_name)

        pipeline = Pipeline(
            [
                BatchTransform(
                    labels=self.labels,
                    func=upload_prediction_to_label_studio,
                    inputs=[self.input__item__has__prediction, self.input__label_studio_project_task],
                    outputs=[self.output__label_studio_project_prediction],
                    chunk_size=self.chunk_size,
                    executor_config=self.executor_config,
                    kwargs=dict(
                        get_project_context=self.get_project_context,
                        primary_keys=self.primary_keys,
                        dt__output__label_studio_project_prediction=dt__output__label_studio_project_prediction,
                        model_version__column=self.model_version__column,
                    ),
                ),
            ]
        )
        return build_compute(ds, catalog, pipeline)


def upload_prediction_to_label_studio_projects(
    df__label_studio_project: pd.DataFrame,
    df__item__has__prediction: pd.DataFrame,
    df__label_studio_project_task: pd.DataFrame,
    idx: IndexDF,
    ls_client: LabelStudio,
    primary_keys: List[str],
    dt__output__label_studio_project_prediction: DataTable,
    model_version__column: str,
) -> pd.DataFrame:
    project_identifiers = (
        set(df__label_studio_project["project_identifier"])
        .union(set(df__item__has__prediction["project_identifier"]))
        .union(set(df__label_studio_project_task["project_identifier"]))
        .union(set(idx["project_identifier"]))
    )
    dfs = []
    for project_identifier in project_identifiers:
        if project_identifier not in set(df__label_studio_project["project_identifier"]):
            logger.info(f"Project {project_identifier} not found in input__label_studio_project. Skipping")
            continue
        project_id = df__label_studio_project[
            df__label_studio_project["project_identifier"] == project_identifier
        ].iloc[0]["project_id"]
        df__item__has__prediction_by_project_identifier = df__item__has__prediction[
            df__item__has__prediction["project_identifier"] == project_identifier
        ]
        df__label_studio_project_task_by_project_identifier = df__label_studio_project_task[
            df__label_studio_project_task["project_identifier"] == project_identifier
        ]
        idx_by_project_identifier = idx[idx["project_identifier"] == project_identifier]

        def _get_project_context(project_id: int = project_id) -> Tuple[LabelStudio, int]:
            return (ls_client, project_id)

        df__res = upload_prediction_to_label_studio(
            df__item__has__prediction=df__item__has__prediction_by_project_identifier,
            df__label_studio_project_task=df__label_studio_project_task_by_project_identifier,
            idx=idx_by_project_identifier,
            get_project_context=_get_project_context,
            primary_keys=primary_keys,
            dt__output__label_studio_project_prediction=dt__output__label_studio_project_prediction,
            model_version__column=model_version__column,
        )
        dfs.append(df__res)
    if len(dfs) == 0:
        dfs_res = pd.DataFrame(columns=primary_keys + ["task_id", "prediction_id", "model_version", "prediction"])
    else:
        dfs_res = pd.concat(dfs, ignore_index=True)
    return dfs_res


@dataclass
class LabelStudioUploadPredictionsToProjects(PipelineStep):
    input__item__has__prediction: PipelineInput
    # prediction имеет такой вид: {"result": [...], "score": 0.}
    input__label_studio_project: PipelineInput
    input__label_studio_project_task: PipelineInput
    output__label_studio_project_prediction: PipelineOutput

    ls_url: str
    api_key: str
    primary_keys: List[str]

    chunk_size: int = 100
    create_table: bool = False
    labels: Optional[Labels] = None
    model_version__column: str = "model_version"
    executor_config: Optional[ExecutorConfig] = None

    def __post_init__(self):
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
        input_item_has_prediction_name = get_pipeline_input_name(self.input__item__has__prediction)
        output_label_studio_project_prediction_name = get_pipeline_output_name(
            self.output__label_studio_project_prediction
        )
        dt__input__has__prediction = ds.get_table(input_item_has_prediction_name)
        assert isinstance(dt__input__has__prediction.table_store, TableStoreDB)
        check_columns_are_in_table(
            ds, self.input__item__has__prediction, self.primary_keys + ["prediction", self.model_version__column]
        )
        check_columns_are_in_table(ds, self.input__label_studio_project_task, self.primary_keys + ["task_id"])
        check_columns_are_in_table(ds, self.input__label_studio_project, ["project_identifier", "project_id"])
        catalog.add_datatable(
            output_label_studio_project_prediction_name,
            Table(
                ds.get_or_create_table(
                    output_label_studio_project_prediction_name,
                    TableStoreDB(
                        dbconn=ds.meta_dbconn,
                        name=output_label_studio_project_prediction_name,
                        data_sql_schema=[
                            column
                            for column in dt__input__has__prediction.primary_schema
                            if column.name in self.primary_keys
                        ]
                        + [
                            Column("task_id", Integer),
                            Column("prediction_id", Integer),
                            Column("model_version", String),
                            Column("prediction", JSON),
                        ],
                        create_table=self.create_table,
                    ),
                ).table_store
            ),
        )
        dt__output__label_studio_project_prediction = ds.get_table(output_label_studio_project_prediction_name)

        pipeline = Pipeline(
            [
                BatchTransform(
                    labels=self.labels,
                    func=upload_prediction_to_label_studio_projects,
                    inputs=[
                        self.input__label_studio_project,
                        self.input__item__has__prediction,
                        self.input__label_studio_project_task,
                    ],
                    outputs=[self.output__label_studio_project_prediction],
                    chunk_size=self.chunk_size,
                    executor_config=self.executor_config,
                    kwargs=dict(
                        ls_client=self.ls_client,
                        primary_keys=self.primary_keys,
                        dt__output__label_studio_project_prediction=dt__output__label_studio_project_prediction,
                        model_version__column=self.model_version__column,
                    ),
                    transform_keys=self.primary_keys,
                ),
            ]
        )
        return build_compute(ds, catalog, pipeline)
