from dataclasses import asdict, dataclass, field, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union, cast

import pandas as pd
from datapipe.compute import (
    Catalog,
    ComputeStep,
    Pipeline,
    PipelineStep,
    Table,
    build_compute,
)
from datapipe.datatable import DataStore, DataTable
from datapipe.executor import ExecutorConfig
from datapipe.run_config import RunConfig
from datapipe.step.batch_transform import DatatableBatchTransform
from datapipe.store.database import TableStoreDB
from datapipe.types import IndexDF, Labels
from pathy import Pathy
from sqlalchemy import JSON, Column
from sqlalchemy.sql.sqltypes import Integer, String

from datapipe_ml.core.datapipe import check_columns_are_in_table, get_datatable, get_pipeline_table_name
from datapipe_ml.frameworks.tensorflow.classification_runner import (
    TF_ClassificationTrainingConfig,
    TrainModelResult,
)
from datapipe_ml.frameworks.tensorflow.classification_runner import TFModelSpec as TFModelSpec  # noqa: F401
from datapipe_ml.training.experiment_tables import (
    TrainExperimentTableNames,
    build_auto_experiment_request_transform,
    build_default_train_config_generate,
    register_train_experiment_tables,
)
from datapipe_ml.training.orchestrator import orchestrate
from datapipe_ml.training.paths import default_tmp_folder, remote_input_path, remote_output_models_path
from datapipe_ml.training.request_runner import run_training_request
from datapipe_ml.training.specs import (
    Algo,
    PreparedData,
    TrainContext,
    TrainingLauncherConfig,
    TrainingLaunchRequest,
    TrainingPathMap,
    TrainingResumeCheckpoint,
    TrainingResumeConfig,
    TrainingSyncConfig,
    build_training_launcher,
)
from datapipe_ml.training.train_config_id import (
    TrainingConfigPreset,
    build_train_config_id,
    train_configs_to_dataframe,
)

TFClassificationTrainConfigItem = Union[TF_ClassificationTrainingConfig, TrainingConfigPreset]


@dataclass
class TensorflowClassificationPreparedData(PreparedData):
    df__frozen_dataset__has__image_gt: pd.DataFrame


@dataclass
class TensorflowClassificationTrainContext(TrainContext):
    clean_checkpoints_after_train: bool
    orchestrator_idx: Optional[IndexDF] = None


@dataclass
class TensorflowClassificationRuntimeConfig:
    models_dir: str
    max_within_time: str
    tmp_folder: str
    model_suffix: str
    dt__model: DataTable
    dt__link: DataTable
    dt__training_status: DataTable
    model_other_primary_keys: List[str]
    model_id__name: str
    frozen_dataset_id__name: str
    clean_checkpoints_after_train: bool
    training_launcher_config: Optional[TrainingLauncherConfig]
    sync_config: Optional[TrainingSyncConfig]
    resume_config: Optional[TrainingResumeConfig]

    @classmethod
    def from_kwargs(cls, kwargs: Dict[str, Any]) -> "TensorflowClassificationRuntimeConfig":
        return cls(
            models_dir=kwargs["models_dir"],
            max_within_time=kwargs["max_within_time"],
            tmp_folder=kwargs["tmp_folder"],
            model_suffix=kwargs["model_suffix"],
            dt__model=kwargs["dt__classification_model"],
            dt__link=kwargs["dt__classification_model_is_trained_on_cls_frozen_dataset"],
            dt__training_status=kwargs["dt__training_status"],
            model_other_primary_keys=kwargs["classification_model_other_primary_keys"],
            model_id__name=kwargs["classification_model_id__name"],
            frozen_dataset_id__name=kwargs["classification_frozen_dataset_id__name"],
            clean_checkpoints_after_train=kwargs["clean_checkpoints_after_train"],
            training_launcher_config=kwargs.get("training_launcher_config"),
            sync_config=kwargs.get("sync_config"),
            resume_config=kwargs.get("resume_config"),
        )

    def build_context(
        self,
        *,
        idx: IndexDF,
        dt__frozen_dataset: DataTable,
        dt__frozen_dataset__has__image_gt: DataTable,
        dt__train_config: Optional[DataTable],
    ) -> TensorflowClassificationTrainContext:
        return TensorflowClassificationTrainContext(
            models_dir=self.models_dir,
            max_within_time=self.max_within_time,
            tmp_folder=self.tmp_folder,
            model_suffix=self.model_suffix,
            dt__model=self.dt__model,
            dt__link=self.dt__link,
            dt__training_status=self.dt__training_status,
            dt__frozen_dataset=dt__frozen_dataset,
            dt__frozen_dataset__has__image_gt=dt__frozen_dataset__has__image_gt,
            dt__train_config=dt__train_config,
            model_other_primary_keys=self.model_other_primary_keys,
            model_id__name=self.model_id__name,
            frozen_dataset_id__name=self.frozen_dataset_id__name,
            clean_checkpoints_after_train=self.clean_checkpoints_after_train,
            orchestrator_idx=idx,
            training_launcher_config=self.training_launcher_config,
            sync_config=self.sync_config,
            resume_config=self.resume_config,
        )


class TFClassificationAlgo(Algo):
    train_config_id_col = "classification_train_config_id"
    train_params_col = "classification_train_config__params"
    frozen_created_at_col = "classification_frozen_dataset__created_at"
    images_count_col = None
    model_row_prefix = "classification_model"

    def check_accelerator(self, train_params: Dict[str, Any]) -> None:
        from datapipe_ml.training.accelerator import cpu_training_allowed

        if cpu_training_allowed(train_params):
            return

        import tensorflow as tf

        if not tf.config.list_physical_devices("GPU"):
            raise ValueError("GPU not found.")

    def prepare_data(self, ctx: TrainContext, idx: IndexDF) -> TensorflowClassificationPreparedData:
        if ctx.dt__frozen_dataset__has__image_gt is None:
            raise ValueError("dt__frozen_dataset__has__image_gt must be provided for TF classification")
        df__frozen_dataset__has__image_gt = ctx.dt__frozen_dataset__has__image_gt.get_data(idx)
        return TensorflowClassificationPreparedData(df__frozen_dataset__has__image_gt=df__frozen_dataset__has__image_gt)

    def build_model_id(self, ctx: TrainContext, idx, train_params: Dict[str, Any]) -> str:
        date = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")
        prefix = (
            "__".join([str(idx.loc[0][k]) for k in ctx.model_other_primary_keys])
            if ctx.model_other_primary_keys
            else ""
        )
        summary = build_tf_classification_train_config_summary(train_params)
        config_id = build_train_config_id(train_params, summary=summary)
        return f"{prefix + ('-' if prefix else '')}{date}_{config_id}{ctx.model_suffix}"

    def select_resume_checkpoint(
        self,
        *,
        manifest_path: Optional[str],
        config: Optional[TrainingResumeConfig],
    ) -> Optional[TrainingResumeCheckpoint]:
        from datapipe_ml.frameworks.tensorflow.checkpoint_selection import select_tf_resume_checkpoint

        return select_tf_resume_checkpoint(manifest_path=manifest_path, config=config)

    def launch_training(
        self,
        ctx: TrainContext,
        idx: IndexDF,
        model_id: str,
        train_params: Dict[str, Any],
        data: PreparedData,
        resume_checkpoint: Optional[TrainingResumeCheckpoint] = None,
    ) -> Any:
        from datapipe_ml.frameworks.tensorflow.classification_runner import (
            TF_ClassificationTrainingConfig,
            train_process,
        )

        cfg = TF_ClassificationTrainingConfig(**train_params)
        resume_checkpoint_filepath = resume_checkpoint.path if resume_checkpoint is not None else None
        resume_checkpoint_epoch = resume_checkpoint.epoch if resume_checkpoint is not None else None
        # ctx has an extra field clean_checkpoints_after_train in TensorflowClassificationTrainContext
        from typing import cast as _cast

        tctx = _cast(TensorflowClassificationTrainContext, ctx)
        d = _cast(TensorflowClassificationPreparedData, data)
        image_paths = tuple(str(path) for path in d.df__frozen_dataset__has__image_gt["image__image_path"].tolist())
        image_rewrites = tuple(
            (path, remote_input_path("classification_images", Path(path).name)) for path in image_paths
        )
        write_models_dir = ctx.training_output_write_dir or ctx.models_dir
        subprocess_sync_config = None if ctx.training_output_write_dir else ctx.sync_config
        persisted_models_dir = ctx.models_dir if ctx.training_output_write_dir else None
        launcher = build_training_launcher(ctx.training_launcher_config)
        return launcher.launch(
            TrainingLaunchRequest.from_path_maps(
                target=train_process,
                args=(
                    d.df__frozen_dataset__has__image_gt,
                    cfg,
                    model_id,
                    write_models_dir,
                    tctx.clean_checkpoints_after_train,
                    str(ctx.tmp_folder),
                    resume_checkpoint_filepath,
                    resume_checkpoint_epoch,
                    subprocess_sync_config,
                    persisted_models_dir,
                ),
                cluster_suffix=model_id,
                inputs=tuple(TrainingPathMap(local_path, remote_path) for local_path, remote_path in image_rewrites),
                outputs=(TrainingPathMap(str(ctx.models_dir), remote_output_models_path()),),
            )
        )

    def select_best(self, raw_result: TrainModelResult, idx: IndexDF) -> Dict[str, Any]:
        if raw_result is None or raw_result.classification_model_id is None:
            traceback_logs = raw_result.traceback_logs if raw_result is not None else None
            raise ValueError(f"Train failed: '{traceback_logs}'")
        return dict(
            model_path=raw_result.model_path,
            class_names=raw_result.class_names,
            type_name="tf.keras",
            classification_model_id=raw_result.classification_model_id,
            preprocess_input_script_path=raw_result.preprocess_input_script_path,
        )

    def collect_checkpoint_paths(self, raw_result: TrainModelResult) -> List[str]:
        if raw_result.model_path is None:
            return []
        return [str(raw_result.model_path)]

    def discover_checkpoint_paths_in_run_dir(self, run_dir: str) -> List[str]:
        from datapipe_ml.frameworks.tensorflow.checkpoint_sync import discover_checkpoint_paths_in_run_dir

        return discover_checkpoint_paths_in_run_dir(run_dir)

    def infer_epoch_from_checkpoint_path(self, path: str) -> Optional[int]:
        from datapipe_ml.frameworks.tensorflow.checkpoint_sync import infer_epoch_from_checkpoint_path

        return infer_epoch_from_checkpoint_path(path)

    def build_model_row(
        self,
        ctx: TrainContext,
        idx: IndexDF,
        model_id: str,
        best: Dict[str, Any],
        train_params: Dict[str, Any],
    ):
        from pandas import DataFrame

        prefix = self.model_row_prefix
        row = {
            **{k: idx.loc[0][k] for k in ctx.model_other_primary_keys},
            ctx.model_id__name: best.get("classification_model_id", model_id),
            f"{prefix}__input_size": tuple(train_params["image_size"]),
            f"{prefix}__model_path": best["model_path"],
            f"{prefix}__type": best["type_name"],
            f"{prefix}__class_names": best["class_names"],
            f"{prefix}__preprocess_input_script_path": best["preprocess_input_script_path"],
        }
        return DataFrame([row])


def train_tf_classification_model(
    ds: DataStore,
    idx: IndexDF,
    input_dts: List[DataTable],
    run_config: Optional[RunConfig] = None,
    kwargs: Optional[Dict[str, Any]] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Request-driven TF classification train (same contract as YOLO).

    Inputs: frozen_dataset, training_request, frozen_dataset__has__image_gt.
    Transform keys are ``training_request_id`` (+ optional model other PKs).
    """
    kwargs = kwargs or {}
    (
        dt__classification_frozen_dataset,
        dt__classification_training_request,
        dt__classification_frozen_dataset__has__image_gt,
    ) = input_dts

    frozen_dataset_id_col = kwargs["classification_frozen_dataset_id__name"]
    df_training_request = dt__classification_training_request.get_data(idx)
    if df_training_request.empty:
        empty = pd.DataFrame()
        return empty, empty, empty
    if frozen_dataset_id_col not in df_training_request.columns:
        raise ValueError(
            f"Training request is missing required column {frozen_dataset_id_col!r}"
        )

    frozen_dataset_id = df_training_request.iloc[0][frozen_dataset_id_col]
    data_idx = IndexDF(pd.DataFrame([{frozen_dataset_id_col: frozen_dataset_id}]))
    df_frozen_dataset = dt__classification_frozen_dataset.get_data(data_idx)

    orch_idx = idx.copy()
    orch_idx[frozen_dataset_id_col] = frozen_dataset_id

    def _train_callable(
        *,
        df_train_config: pd.DataFrame,
        max_within_time: Optional[str],
        force_training: bool,
        **_: Any,
    ) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        runtime = TensorflowClassificationRuntimeConfig.from_kwargs(kwargs)
        ctx = runtime.build_context(
            idx=orch_idx,
            dt__frozen_dataset=dt__classification_frozen_dataset,
            dt__frozen_dataset__has__image_gt=dt__classification_frozen_dataset__has__image_gt,
            dt__train_config=None,
        )
        if max_within_time is not None:
            ctx = replace(ctx, max_within_time=max_within_time)
        out = orchestrate(
            orch_idx,
            ctx,
            TFClassificationAlgo(),
            df_train_config=df_train_config,
            force_training=force_training,
        )
        return (out.df__model, out.df__link, out.df__training_status)

    return cast(
        Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame],
        run_training_request(
            df_frozen_dataset=df_frozen_dataset,
            df_training_request=df_training_request,
            df_class_names=pd.DataFrame(),
            df_resized_images=pd.DataFrame(),
            df_yolo_txt=pd.DataFrame(),
            train_callable=_train_callable,
            train_config_id_col="classification_train_config_id",
            train_config_params_col="classification_train_config__params",
            frozen_dataset_id_col=frozen_dataset_id_col,
            dt_training_status=kwargs.get("dt__training_status"),
        ),
    )


def get_tf_classification_model_name(train_params: Dict[str, Any]) -> str:
    if train_params.get("model_spec") is not None:
        factory = train_params["model_spec"]["factory"]
        return factory.rsplit(":", maxsplit=1)[-1]
    return str(train_params["arch"])


def build_tf_classification_train_config_summary(train_params: Dict[str, Any]) -> str:
    model_name = get_tf_classification_model_name(train_params)
    width, height = train_params["image_size"]
    return (
        f"{model_name}-size{width}x{height}-batch{train_params['batch_size']}-epochs{train_params['epochs']}"
    )


def get_tf_classification_train_config_id(config: TF_ClassificationTrainingConfig) -> str:
    params = asdict(config)
    return build_train_config_id(params, summary=build_tf_classification_train_config_summary(params))


def get_tf_classification_train_configs(
    tf_classification_train_configs: List[TFClassificationTrainConfigItem],
):
    yield train_configs_to_dataframe(
        tf_classification_train_configs,
        id_column="classification_train_config_id",
        params_column="classification_train_config__params",
        summary_builder=build_tf_classification_train_config_summary,
        config_type="tf_classification",
    )


this_folder = Path(__file__).parent


@dataclass
class Train_Tensorflow_ClassificationModel(PipelineStep):
    input__classification_frozen_dataset: str
    input__classification_frozen_dataset__has__image_gt: str
    output__tf_classification_train_config: str
    output__tf_classification_custom_train_config: str
    output__classification_training_request: str
    output__classification_model: str
    output__classification_model_is_trained_on_cls_frozen_dataset: str
    output__training_status: str
    working_dir: str
    tf_classification_train_configs: List[TFClassificationTrainConfigItem]
    primary_keys: List[str]
    # yolov5_classification_train_config_id: str = 'yolov5_classification_train_config_id',
    # classification_model_id: str = 'classification_model_id',
    max_within_time: str = "1w"
    create_table: bool = False
    labels: Optional[Labels] = None
    executor_config: Optional[ExecutorConfig] = None
    classification_model_primary_keys: Optional[List[str]] = None
    classification_model_id__name: str = "classification_model_id"
    classification_frozen_dataset_id__name: str = "classification_frozen_dataset_id"
    clean_checkpoints_after_train: bool = False
    tmp_folder: str = field(default_factory=default_tmp_folder)  # When used cloud images, store them to this folder
    model_suffix: str = "_default"
    training_launcher_config: Optional[TrainingLauncherConfig] = None
    sync_config: Optional[TrainingSyncConfig] = None
    resume_config: Optional[TrainingResumeConfig] = None
    filedir_fsspec_kwargs: dict[str, Any] | None = None

    def build_compute(self, ds: DataStore, catalog: Catalog) -> List[ComputeStep]:
        if self.classification_model_primary_keys is None:
            self.classification_model_primary_keys = [self.classification_model_id__name]
        if self.classification_model_id__name not in self.classification_model_primary_keys:
            raise ValueError(
                f"{self.classification_model_id__name!r} must be present in classification_model_primary_keys"
            )
        classification_model_other_primary_keys = [
            x for x in self.classification_model_primary_keys if x != self.classification_model_id__name
        ]
        check_columns_are_in_table(
            ds,
            self.input__classification_frozen_dataset,
            classification_model_other_primary_keys + [self.classification_frozen_dataset_id__name],
        )
        check_columns_are_in_table(
            ds,
            self.input__classification_frozen_dataset__has__image_gt,
            classification_model_other_primary_keys + ["subset_id", "image__image_path", "label"],
        )
        dt__input__classification_frozen_dataset__has__image_gt = get_datatable(
            ds,
            self.input__classification_frozen_dataset__has__image_gt,
        )
        classification_model_primary_columns = [
            column
            for column in dt__input__classification_frozen_dataset__has__image_gt.primary_schema
            if column.name in classification_model_other_primary_keys
        ] + [Column(self.classification_model_id__name, String, primary_key=True)]

        default_train_config = get_pipeline_table_name(self.output__tf_classification_train_config)
        custom_train_config = get_pipeline_table_name(self.output__tf_classification_custom_train_config)
        training_request = get_pipeline_table_name(self.output__classification_training_request)
        register_train_experiment_tables(
            ds=ds,
            catalog=catalog,
            tables=TrainExperimentTableNames(
                default_train_config=default_train_config,
                custom_train_config=custom_train_config,
                train_experiment_request=training_request,
            ),
            train_config_id_col="classification_train_config_id",
            train_config_params_col="classification_train_config__params",
            frozen_dataset_id_col=self.classification_frozen_dataset_id__name,
            create_table=self.create_table,
        )
        # ---
        catalog.add_datatable(
            get_pipeline_table_name(self.output__classification_model),
            Table(
                ds.get_or_create_table(
                    get_pipeline_table_name(self.output__classification_model),
                    TableStoreDB(
                        dbconn=ds.meta_dbconn,
                        name=get_pipeline_table_name(self.output__classification_model),
                        data_sql_schema=classification_model_primary_columns
                        + [
                            Column("classification_model__input_size", JSON),
                            Column("classification_model__model_path", String),
                            Column("classification_model__type", String),
                            Column("classification_model__class_names", JSON),
                            Column("classification_model__preprocess_input_script_path", String),
                            Column("training_request_id", String, nullable=True),
                        ],
                        create_table=self.create_table,
                    ),
                ).table_store
            ),
        )
        catalog.add_datatable(
            get_pipeline_table_name(self.output__classification_model_is_trained_on_cls_frozen_dataset),
            Table(
                ds.get_or_create_table(
                    get_pipeline_table_name(self.output__classification_model_is_trained_on_cls_frozen_dataset),
                    TableStoreDB(
                        dbconn=ds.meta_dbconn,
                        name=get_pipeline_table_name(self.output__classification_model_is_trained_on_cls_frozen_dataset),
                        data_sql_schema=classification_model_primary_columns
                        + [
                            Column(self.classification_frozen_dataset_id__name, String, primary_key=True),
                            Column("classification_train_config_id", String, primary_key=True),
                            Column("classification_train_config__params", JSON),
                            Column("training_request_id", String, nullable=True),
                        ],
                        create_table=self.create_table,
                    ),
                ).table_store
            ),
        )
        catalog.add_datatable(
            get_pipeline_table_name(self.output__training_status),
            Table(
                ds.get_or_create_table(
                    get_pipeline_table_name(self.output__training_status),
                    TableStoreDB(
                        dbconn=ds.meta_dbconn,
                        name=get_pipeline_table_name(self.output__training_status),
                        data_sql_schema=[
                            Column("training_status_id", String, primary_key=True),
                            Column("training_status__run_key", String),
                            Column("training_status__launcher_type", String),
                            Column("training_status__launcher_config", JSON),
                            Column("training_status__launcher_state", JSON),
                        ]
                        + classification_model_primary_columns[:-1]
                        + [
                            Column(self.classification_frozen_dataset_id__name, String),
                            Column("classification_train_config_id", String),
                            Column(self.classification_model_id__name, String),
                            Column("training_status__models_dir", String),
                            Column("training_status__run_dir", String),
                            Column("training_status__status", String),
                            Column("training_status__started_at", String),
                            Column("training_status__finished_at", String),
                            Column("training_status__attempt", Integer),
                            Column("training_status__manifest_path", String),
                            Column("training_status__error", String),
                            Column("training_status__owner_id", String),
                            Column("training_status__heartbeat_at", String),
                            Column("training_status__lease_expires_at", String),
                            Column("training_request_id", String, nullable=True),
                        ],
                        create_table=self.create_table,
                    ),
                ).table_store
            ),
        )
        # ---
        pipeline = Pipeline(
            [
                build_default_train_config_generate(
                    func=get_tf_classification_train_configs,
                    default_train_config_table=default_train_config,
                    kwargs=dict(tf_classification_train_configs=self.tf_classification_train_configs),
                    labels=self.labels,
                ),
                build_auto_experiment_request_transform(
                    frozen_dataset_table=get_pipeline_table_name(self.input__classification_frozen_dataset),
                    default_train_config_table=default_train_config,
                    train_experiment_request_table=training_request,
                    frozen_dataset_id_col=self.classification_frozen_dataset_id__name,
                    train_config_id_col="classification_train_config_id",
                    train_config_params_col="classification_train_config__params",
                    train_config_type="tf_classification",
                    max_within_time=self.max_within_time,
                    labels=self.labels,
                ),
                DatatableBatchTransform(
                    func=train_tf_classification_model,
                    inputs=[
                        self.input__classification_frozen_dataset,
                        self.output__classification_training_request,
                        self.input__classification_frozen_dataset__has__image_gt,
                    ],
                    outputs=[
                        self.output__classification_model,
                        self.output__classification_model_is_trained_on_cls_frozen_dataset,
                        self.output__training_status,
                    ],
                    transform_keys=classification_model_other_primary_keys + ["training_request_id"],
                    chunk_size=1,
                    kwargs=dict(
                        models_dir=str(Pathy.fluid(self.working_dir) / "models"),
                        max_within_time=self.max_within_time,
                        dt__classification_model=get_datatable(ds, self.output__classification_model),
                        dt__classification_model_is_trained_on_cls_frozen_dataset=get_datatable(
                            ds,
                            self.output__classification_model_is_trained_on_cls_frozen_dataset,
                        ),
                        dt__training_status=get_datatable(ds, self.output__training_status),
                        classification_model_other_primary_keys=classification_model_other_primary_keys,
                        classification_model_id__name=self.classification_model_id__name,
                        classification_frozen_dataset_id__name=self.classification_frozen_dataset_id__name,
                        clean_checkpoints_after_train=self.clean_checkpoints_after_train,
                        tmp_folder=self.tmp_folder,
                        model_suffix=self.model_suffix,
                        training_launcher_config=self.training_launcher_config,
                        sync_config=self.sync_config,
                        resume_config=self.resume_config,
                    ),
                    labels=self.labels,
                    executor_config=self.executor_config,
                ),
            ]
        )
        return build_compute(ds, catalog, pipeline)
