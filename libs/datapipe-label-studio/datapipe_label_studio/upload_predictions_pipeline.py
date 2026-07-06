import copy
import json
from dataclasses import dataclass, field
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
    enable_prelabeling_with_model_version,
    get_ls_client,
    resolve_project_id,
)
from datapipe_label_studio.upload_tasks_pipeline import logger
from datapipe_label_studio.utils import check_columns_are_in_table


MODEL_VERSION_SEPARATOR = "/"


def _make_jsonable(value: object) -> object:
    return json.loads(json.dumps(value, default=str))


def _validate_prediction_primary_keys(primary_keys: List[str], model_keys: List[str]) -> None:
    if len(model_keys) == 0:
        raise ValueError("model_keys must not be empty")
    missing_keys = [key for key in model_keys if key not in primary_keys]
    if missing_keys:
        raise ValueError(
            f"{model_keys=} must be included in {primary_keys=} "
            "to support deletion of stale predictions when model version changes. "
            f"Missing keys: {missing_keys}"
        )


def _prediction_task_join_keys(primary_keys: List[str], model_keys: List[str]) -> List[str]:
    model_keys_set = set(model_keys)
    return [key for key in primary_keys if key not in model_keys_set]


def _compose_model_version(row: pd.Series, model_keys: List[str]) -> str:
    return MODEL_VERSION_SEPARATOR.join(str(row[key]) for key in model_keys)


def _compose_model_version_from_df(df: pd.DataFrame, model_keys: List[str]) -> str:
    if len(df) > 1:
        raise ValueError(
            f"input__best_model must contain exactly one row, got {len(df)} rows"
        )
    return _compose_model_version(df.iloc[0], model_keys)


def _assign_model_version_column(df: pd.DataFrame, model_keys: List[str]) -> pd.Series:
    if len(model_keys) == 1:
        return df[model_keys[0]].astype(str)
    return df[model_keys].astype(str).agg(MODEL_VERSION_SEPARATOR.join, axis=1)


def upload_prediction_to_label_studio(
    df__item__has__prediction: pd.DataFrame,
    df__label_studio_project_task: pd.DataFrame,
    idx: IndexDF,
    get_project_context: Callable[[], Tuple[LabelStudio, int]],
    primary_keys: List[str],
    dt__output__label_studio_project_prediction: DataTable,
    model_keys: List[str],
) -> pd.DataFrame:
    """
    Добавляет в LS предсказания.
    """
    task_join_keys = _prediction_task_join_keys(primary_keys, model_keys)
    df = pd.merge(df__item__has__prediction, df__label_studio_project_task, on=task_join_keys)
    if (df__item__has__prediction.empty and df__label_studio_project_task.empty) and idx.empty:
        return pd.DataFrame(columns=primary_keys + ["task_id", "prediction"])

    client, project_id = get_project_context()
    delete_lookup_idx = (
        data_to_index(idx, primary_keys)
        if set(primary_keys).issubset(idx.columns)
        else data_to_index(idx, task_join_keys)
    )
    df_existing_prediction_to_be_deleted = dt__output__label_studio_project_prediction.get_data(
        idx=delete_lookup_idx
    )
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

    df["model_version"] = _assign_model_version_column(df, model_keys)
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
    return df[list(dict.fromkeys(primary_keys + ["task_id", "prediction_id", "prediction"]))]


def _set_current_model_version_row(
    client: LabelStudio,
    project_id: int,
    model_version: str,
) -> pd.DataFrame:
    enable_prelabeling_with_model_version(client, project_id, model_version)
    return pd.DataFrame({"project_id": [project_id], "model_version": [model_version]})


def _model_version_from_best_model_df(
    df__best_model: pd.DataFrame,
    model_keys: List[str],
) -> str:
    return _compose_model_version_from_df(df__best_model, model_keys)


def _for_each_label_studio_project(
    df__label_studio_project: pd.DataFrame,
    idx: IndexDF,
    *dfs_with_project_identifier: pd.DataFrame,
):
    project_identifiers = set(df__label_studio_project["project_identifier"])
    if "project_identifier" in idx.columns:
        project_identifiers |= set(idx["project_identifier"])
    for df in dfs_with_project_identifier:
        if "project_identifier" in df.columns:
            project_identifiers |= set(df["project_identifier"])

    known_project_identifiers = set(df__label_studio_project["project_identifier"])
    for project_identifier in project_identifiers:
        if project_identifier not in known_project_identifiers:
            logger.info(f"Project {project_identifier} not found in input__label_studio_project. Skipping")
            continue
        project_id = int(
            df__label_studio_project[
                df__label_studio_project["project_identifier"] == project_identifier
            ].iloc[0]["project_id"]
        )
        idx_by_project_identifier = (
            idx[idx["project_identifier"] == project_identifier]
            if "project_identifier" in idx.columns
            else idx
        )
        yield project_identifier, project_id, idx_by_project_identifier


def set_label_studio_current_model_version(
    df__best_model: pd.DataFrame,
    idx: IndexDF,
    get_project_context: Callable[[], Tuple[LabelStudio, int]],
    model_keys: List[str],
) -> pd.DataFrame:
    """
    Включает prelabeling в LS и сохраняет выбранную model_version для проекта.
    """
    if df__best_model.empty and idx.empty:
        return pd.DataFrame(columns=["project_id", "model_version"])

    model_version = _model_version_from_best_model_df(df__best_model, model_keys)
    client, project_id = get_project_context()
    return _set_current_model_version_row(client, project_id, model_version)


def set_label_studio_current_model_version_for_projects(
    df__label_studio_project: pd.DataFrame,
    df__best_model: pd.DataFrame,
    idx: IndexDF,
    ls_client: LabelStudio,
    model_keys: List[str],
) -> pd.DataFrame:
    dfs = []
    for project_identifier, project_id, idx_by_project_identifier in _for_each_label_studio_project(
        df__label_studio_project,
        idx,
        df__best_model,
    ):
        df__best_model_by_project_identifier = df__best_model[
            df__best_model["project_identifier"] == project_identifier
        ]
        if df__best_model_by_project_identifier.empty and idx_by_project_identifier.empty:
            continue

        def _get_project_context(project_id: int = project_id) -> Tuple[LabelStudio, int]:
            return (ls_client, project_id)

        dfs.append(
            set_label_studio_current_model_version(
                df__best_model=df__best_model_by_project_identifier,
                idx=idx_by_project_identifier,
                get_project_context=_get_project_context,
                model_keys=model_keys,
            )
        )
    if len(dfs) == 0:
        return pd.DataFrame(columns=["project_id", "model_version"])
    return pd.concat(dfs, ignore_index=True)


def _register_current_model_version_table(
    ds: DataStore,
    catalog: Catalog,
    output_name: str,
    create_table: bool,
) -> None:
    catalog.add_datatable(
        output_name,
        Table(
            ds.get_or_create_table(
                output_name,
                TableStoreDB(
                    dbconn=ds.meta_dbconn,
                    name=output_name,
                    data_sql_schema=[
                        Column("project_id", Integer, primary_key=True),
                        Column("model_version", String),
                    ],
                    create_table=create_table,
                ),
            ).table_store
        ),
    )


@dataclass
class LabelStudioUploadPredictions(PipelineStep):
    input__item__has__prediction: PipelineInput
    # prediction имеет такой вид: {"result": [...], "score": 0.}
    input__label_studio_project_task: PipelineInput
    input__best_model: PipelineInput
    output__label_studio_current_model_version: PipelineOutput
    output__label_studio_project_prediction: PipelineOutput

    ls_url: str
    api_key: str
    project_identifier: Union[str, int]  # project_title or id
    primary_keys: List[str]

    chunk_size: int = 100
    create_table: bool = False
    labels: Optional[Labels] = None
    model_keys: List[str] = field(default_factory=lambda: ["model_version"])
    executor_config: Optional[ExecutorConfig] = None

    def __post_init__(self):
        if isinstance(self.project_identifier, str):
            assert len(self.project_identifier) <= 50
        _validate_prediction_primary_keys(self.primary_keys, self.model_keys)

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
            ds, self.input__item__has__prediction, self.primary_keys + ["prediction"]
        )
        task_join_keys = _prediction_task_join_keys(self.primary_keys, self.model_keys)
        check_columns_are_in_table(ds, self.input__label_studio_project_task, task_join_keys + ["task_id"])
        input_columns = {
            col.name: col for col in dt__input__has__prediction.table_store.get_schema()
        }
        primary_schema_columns = []
        for key in self.primary_keys:
            column = copy.copy(input_columns[key])
            column.primary_key = True
            primary_schema_columns.append(column)
        catalog.add_datatable(
            output_label_studio_project_prediction_name,
            Table(
                ds.get_or_create_table(
                    output_label_studio_project_prediction_name,
                    TableStoreDB(
                        dbconn=ds.meta_dbconn,
                        name=output_label_studio_project_prediction_name,
                        data_sql_schema=primary_schema_columns
                        + [
                            column
                            for column in (
                                Column("task_id", Integer),
                                Column("prediction_id", Integer),
                                Column("prediction", JSON),
                            )
                            if column.name not in {col.name for col in primary_schema_columns}
                        ],
                        create_table=self.create_table,
                    ),
                ).table_store
            ),
        )
        dt__output__label_studio_project_prediction = ds.get_table(output_label_studio_project_prediction_name)
        check_columns_are_in_table(
            ds, self.input__best_model, self.model_keys
        )
        output_current_model_version_name = get_pipeline_output_name(
            self.output__label_studio_current_model_version
        )
        _register_current_model_version_table(
            ds,
            catalog,
            output_current_model_version_name,
            self.create_table,
        )

        pipeline = Pipeline(
            [
                BatchTransform(
                    labels=self.labels,
                    func=upload_prediction_to_label_studio,
                    inputs=[self.input__item__has__prediction, self.input__label_studio_project_task],
                    outputs=[self.output__label_studio_project_prediction],
                    transform_keys=task_join_keys,
                    chunk_size=self.chunk_size,
                    executor_config=self.executor_config,
                    kwargs=dict(
                        get_project_context=self.get_project_context,
                        primary_keys=self.primary_keys,
                        dt__output__label_studio_project_prediction=dt__output__label_studio_project_prediction,
                        model_keys=self.model_keys,
                    ),
                ),
                BatchTransform(
                    labels=self.labels,
                    func=set_label_studio_current_model_version,
                    inputs=[self.input__best_model],
                    outputs=[self.output__label_studio_current_model_version],
                    transform_keys=self.model_keys,
                    chunk_size=1,
                    executor_config=self.executor_config,
                    kwargs=dict(
                        get_project_context=self.get_project_context,
                        model_keys=self.model_keys,
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
    model_keys: List[str],
) -> pd.DataFrame:
    dfs = []
    for project_identifier, project_id, idx_by_project_identifier in _for_each_label_studio_project(
        df__label_studio_project,
        idx,
        df__item__has__prediction,
        df__label_studio_project_task,
    ):
        df__item__has__prediction_by_project_identifier = df__item__has__prediction[
            df__item__has__prediction["project_identifier"] == project_identifier
        ]
        df__label_studio_project_task_by_project_identifier = df__label_studio_project_task[
            df__label_studio_project_task["project_identifier"] == project_identifier
        ]

        def _get_project_context(project_id: int = project_id) -> Tuple[LabelStudio, int]:
            return (ls_client, project_id)

        df__res = upload_prediction_to_label_studio(
            df__item__has__prediction=df__item__has__prediction_by_project_identifier,
            df__label_studio_project_task=df__label_studio_project_task_by_project_identifier,
            idx=idx_by_project_identifier,
            get_project_context=_get_project_context,
            primary_keys=primary_keys,
            dt__output__label_studio_project_prediction=dt__output__label_studio_project_prediction,
            model_keys=model_keys,
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
    input__best_model: PipelineInput
    output__label_studio_current_model_version: PipelineOutput
    output__label_studio_project_prediction: PipelineOutput

    ls_url: str
    api_key: str
    primary_keys: List[str]

    chunk_size: int = 100
    create_table: bool = False
    labels: Optional[Labels] = None
    model_keys: List[str] = field(default_factory=lambda: ["model_version"])
    executor_config: Optional[ExecutorConfig] = None

    def __post_init__(self):
        _validate_prediction_primary_keys(self.primary_keys, self.model_keys)
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
            ds, self.input__item__has__prediction, self.primary_keys + ["prediction"]
        )
        task_join_keys = _prediction_task_join_keys(self.primary_keys, self.model_keys)
        check_columns_are_in_table(ds, self.input__label_studio_project_task, task_join_keys + ["task_id"])
        check_columns_are_in_table(ds, self.input__label_studio_project, ["project_identifier", "project_id"])
        input_columns = {
            col.name: col for col in dt__input__has__prediction.table_store.get_schema()
        }
        primary_schema_columns = []
        for key in self.primary_keys:
            column = copy.copy(input_columns[key])
            column.primary_key = True
            primary_schema_columns.append(column)
        catalog.add_datatable(
            output_label_studio_project_prediction_name,
            Table(
                ds.get_or_create_table(
                    output_label_studio_project_prediction_name,
                    TableStoreDB(
                        dbconn=ds.meta_dbconn,
                        name=output_label_studio_project_prediction_name,
                        data_sql_schema=primary_schema_columns
                        + [
                            column
                            for column in (
                                Column("task_id", Integer),
                                Column("prediction_id", Integer),
                                Column("prediction", JSON),
                            )
                            if column.name not in {col.name for col in primary_schema_columns}
                        ],
                        create_table=self.create_table,
                    ),
                ).table_store
            ),
        )
        dt__output__label_studio_project_prediction = ds.get_table(output_label_studio_project_prediction_name)
        check_columns_are_in_table(
            ds,
            self.input__best_model,
            ["project_identifier", *self.model_keys],
        )
        output_current_model_version_name = get_pipeline_output_name(
            self.output__label_studio_current_model_version
        )
        _register_current_model_version_table(
            ds,
            catalog,
            output_current_model_version_name,
            self.create_table,
        )

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
                        model_keys=self.model_keys,
                    ),
                    transform_keys=task_join_keys,
                ),
                BatchTransform(
                    labels=self.labels,
                    func=set_label_studio_current_model_version_for_projects,
                    inputs=[self.input__label_studio_project, self.input__best_model],
                    outputs=[self.output__label_studio_current_model_version],
                    chunk_size=1,
                    executor_config=self.executor_config,
                    kwargs=dict(
                        ls_client=self.ls_client,
                        model_keys=self.model_keys,
                    ),
                    transform_keys=["project_identifier"],
                ),
            ]
        )
        return build_compute(ds, catalog, pipeline)
