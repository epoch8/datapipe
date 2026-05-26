import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

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
from datapipe.step.batch_generate import BatchGenerate
from datapipe.step.batch_transform import DatatableBatchTransform
from datapipe.store.database import TableStoreDB
from datapipe.types import IndexDF, Labels
from pathy import Pathy
from sqlalchemy import JSON, Column
from sqlalchemy.sql.sqltypes import Boolean, String

from datapipe_ml.core.datapipe import check_columns_are_in_table
from datapipe_ml.frameworks.tensorflow.classification_runner import (
    TF_ClassificationTrainingConfig,
)
from datapipe_ml.frameworks.tensorflow.classification_runner import (
    TFModelSpec as TFModelSpec,
)
from datapipe_ml.frameworks.tensorflow.classification_runner import (
    TrainModelResult,
)

# Import concrete pieces for v5
from datapipe_ml.training.orchestrator import orchestrate
from datapipe_ml.training.specs import (
    Algo,
    PreparedData,
    TrainContext,
    TrainingLauncherConfig,
    TrainingLaunchRequest,
    TrainingPathMap,
    build_training_launcher,
)


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
    model_other_primary_keys: List[str]
    model_id__name: str
    frozen_dataset_id__name: str
    clean_checkpoints_after_train: bool
    training_launcher_config: Optional[TrainingLauncherConfig]

    @classmethod
    def from_kwargs(cls, kwargs: Dict[str, Any]) -> "TensorflowClassificationRuntimeConfig":
        return cls(
            models_dir=kwargs["models_dir"],
            max_within_time=kwargs["max_within_time"],
            tmp_folder=kwargs["tmp_folder"],
            model_suffix=kwargs["model_suffix"],
            dt__model=kwargs["dt__classification_model"],
            dt__link=kwargs["dt__classification_model_is_trained_on_cls_frozen_dataset"],
            model_other_primary_keys=kwargs["classification_model_other_primary_keys"],
            model_id__name=kwargs["classification_model_id__name"],
            frozen_dataset_id__name=kwargs["classification_frozen_dataset_id__name"],
            clean_checkpoints_after_train=kwargs["clean_checkpoints_after_train"],
            training_launcher_config=kwargs.get("training_launcher_config"),
        )

    def build_context(
        self,
        *,
        idx: IndexDF,
        dt__frozen_dataset: DataTable,
        dt__frozen_dataset__has__image_gt: DataTable,
        dt__train_config: DataTable,
    ) -> TensorflowClassificationTrainContext:
        return TensorflowClassificationTrainContext(
            models_dir=self.models_dir,
            max_within_time=self.max_within_time,
            tmp_folder=self.tmp_folder,
            model_suffix=self.model_suffix,
            dt__model=self.dt__model,
            dt__link=self.dt__link,
            dt__frozen_dataset=dt__frozen_dataset,
            dt__frozen_dataset__has__image_gt=dt__frozen_dataset__has__image_gt,
            dt__train_config=dt__train_config,
            model_other_primary_keys=self.model_other_primary_keys,
            model_id__name=self.model_id__name,
            frozen_dataset_id__name=self.frozen_dataset_id__name,
            clean_checkpoints_after_train=self.clean_checkpoints_after_train,
            orchestrator_idx=idx,
            training_launcher_config=self.training_launcher_config,
        )


class TFClassificationAlgo(Algo):
    train_config_id_col = "classification_train_config_id"
    train_params_col = "classification_train_config__params"
    frozen_created_at_col = "classification_frozen_dataset__created_at"
    images_count_col = None
    model_row_prefix = "classification_model"

    def check_accelerator(self, train_params: Dict[str, Any]) -> None:
        pass
        # import tensorflow as tf

        # if not tf.test.is_gpu_available():
        #     raise ValueError("GPU not found.")

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
        arch = get_tf_classification_model_name(train_params)
        size = max(train_params["image_size"])
        return f"{prefix + ('-' if prefix else '')}{date}_{arch}{size}{ctx.model_suffix}"

    def launch_training(
        self,
        ctx: TrainContext,
        idx: IndexDF,
        model_id: str,
        train_params: Dict[str, Any],
        data: PreparedData,
    ) -> Any:
        from datapipe_ml.frameworks.tensorflow.classification_runner import (
            TF_ClassificationTrainingConfig,
            train_process,
        )

        cfg = TF_ClassificationTrainingConfig(**train_params)
        # ctx has an extra field clean_checkpoints_after_train in TensorflowClassificationTrainContext
        from typing import cast as _cast

        tctx = _cast(TensorflowClassificationTrainContext, ctx)
        d = _cast(TensorflowClassificationPreparedData, data)
        image_paths = tuple(str(path) for path in d.df__frozen_dataset__has__image_gt["image__image_path"].tolist())
        image_rewrites = tuple(
            (path, f"/workspace/datapipe_ml/input/classification_images/{Path(path).name}") for path in image_paths
        )
        launcher = build_training_launcher(ctx.training_launcher_config)
        return launcher.launch(
            TrainingLaunchRequest.from_path_maps(
                target=train_process,
                args=(
                    d.df__frozen_dataset__has__image_gt,
                    cfg,
                    model_id,
                    ctx.models_dir,
                    tctx.clean_checkpoints_after_train,
                    str(ctx.tmp_folder),
                ),
                cluster_suffix=model_id,
                inputs=tuple(TrainingPathMap(local_path, remote_path) for local_path, remote_path in image_rewrites),
                outputs=(TrainingPathMap(str(ctx.models_dir), "/workspace/datapipe_ml/output/models"),),
            )
        )

    def select_best(self, raw_result: TrainModelResult, idx: IndexDF) -> Dict[str, Any]:
        if raw_result is None or raw_result.classification_model_id is None:
            traceback_logs = raw_result.traceback_logs if raw_result is not None else None
            raise ValueError(f"Train failed: '{traceback_logs}'")
        return dict(
            model_path=raw_result.model_path,
            class_names=raw_result.class_names,
            score_threshold=0.5,  # not used in TF classification, placeholder
            type_name="tf.keras",
            classification_model_id=raw_result.classification_model_id,
            preprocess_input_script_path=raw_result.preprocess_input_script_path,
        )

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
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    kwargs = kwargs or {}
    (
        dt__classification_frozen_dataset,
        dt__classification_frozen_dataset__has__image_gt,
        dt__tf_classification_train_config,
    ) = input_dts

    runtime = TensorflowClassificationRuntimeConfig.from_kwargs(kwargs)
    ctx = runtime.build_context(
        idx=idx,
        dt__frozen_dataset=dt__classification_frozen_dataset,
        dt__frozen_dataset__has__image_gt=dt__classification_frozen_dataset__has__image_gt,
        dt__train_config=dt__tf_classification_train_config,
    )

    algo = TFClassificationAlgo()
    out = orchestrate(idx, ctx, algo)
    return (out.df__model, out.df__link)


def get_tf_classification_model_name(train_params: Dict[str, Any]) -> str:
    if train_params.get("model_spec") is not None:
        factory = train_params["model_spec"]["factory"]
        return factory.rsplit(":", maxsplit=1)[-1]
    return str(train_params["arch"])


def get_tf_classification_train_config_id(config: TF_ClassificationTrainingConfig) -> str:
    params = asdict(config)
    model_name = get_tf_classification_model_name(params)
    model_spec = params.get("model_spec")
    model_spec_id = ""
    if model_spec is not None:
        model_spec_id = "-model_spec" + json.dumps(model_spec, sort_keys=True, default=str)
    return (
        f"{model_name}-size{config.image_size[0]}x{config.image_size[1]}-epochs{config.epochs}-"
        f"batch{config.batch_size}-aug{config.augmentations}-seed{config.seed}-init_lr{config.init_lr}-"
        f"reduce_lr_patience{config.reduce_lr_patience}-reduce_lr_factor{config.reduce_lr_factor}-"
        f"early_stopping_patience{config.early_stopping_patience}-label_smoothing{config.label_smoothing}"
        f"-class_weight-{config.class_weight}{model_spec_id}"
    )


def get_tf_classification_train_configs(tf_classification_train_configs: List[TF_ClassificationTrainingConfig]):
    dfs__train_config = []
    for x in tf_classification_train_configs:
        dfs__train_config.append(
            dict(
                classification_train_config_id=get_tf_classification_train_config_id(x),
                classification_train_config__params=asdict(x),
                classification_train_config__train=True,  # TODO: смотреть на саму табличку в наличии галки
            )
        )
    yield pd.DataFrame(dfs__train_config)


this_folder = Path(__file__).parent


@dataclass
class Train_Tensorflow_ClassificationModel(PipelineStep):
    input__classification_frozen_dataset: str
    input__classification_frozen_dataset__has__image_gt: str
    output__tf_classification_train_config: str
    output__classification_model: str
    output__classification_model_is_trained_on_cls_frozen_dataset: str
    working_dir: str
    tf_classification_train_configs: List[TF_ClassificationTrainingConfig]
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
    tmp_folder: str = "/tmp/"  # When used cloud images, store them to this folder
    model_suffix: str = "_default"
    training_launcher_config: Optional[TrainingLauncherConfig] = None

    def build_compute(self, ds: DataStore, catalog: Catalog) -> List[ComputeStep]:
        if self.classification_model_primary_keys is None:
            self.classification_model_primary_keys = [self.classification_model_id__name]
        assert self.classification_model_id__name in self.classification_model_primary_keys
        classification_model_other_primary_keys = [
            x for x in self.classification_model_primary_keys if x != self.classification_model_id__name
        ]
        check_columns_are_in_table(
            ds,
            self.input__classification_frozen_dataset__has__image_gt,
            classification_model_other_primary_keys + ["subset_id", "image__image_path", "label"],
        )
        dt__input__classification_frozen_dataset__has__image_gt = ds.get_table(
            self.input__classification_frozen_dataset__has__image_gt
        )
        classification_model_primary_columns = [
            column
            for column in dt__input__classification_frozen_dataset__has__image_gt.primary_schema
            if column.name in classification_model_other_primary_keys
        ] + [Column(self.classification_model_id__name, String, primary_key=True)]
        # ---
        catalog.add_datatable(
            self.output__tf_classification_train_config,
            Table(
                ds.get_or_create_table(
                    self.output__tf_classification_train_config,
                    TableStoreDB(
                        dbconn=ds.meta_dbconn,
                        name=self.output__tf_classification_train_config,
                        data_sql_schema=[
                            Column("classification_train_config_id", String, primary_key=True),
                            Column("classification_train_config__params", JSON),
                            Column("classification_train_config__train", Boolean),
                        ],
                        create_table=self.create_table,
                    ),
                ).table_store
            ),
        )
        # ---
        catalog.add_datatable(
            self.output__classification_model,
            Table(
                ds.get_or_create_table(
                    self.output__classification_model,
                    TableStoreDB(
                        dbconn=ds.meta_dbconn,
                        name=self.output__classification_model,
                        data_sql_schema=classification_model_primary_columns
                        + [
                            Column("classification_model__input_size", JSON),
                            Column("classification_model__model_path", String),
                            Column("classification_model__type", String),
                            Column("classification_model__class_names", JSON),
                            Column("classification_model__preprocess_input_script_path", String),
                        ],
                        create_table=self.create_table,
                    ),
                ).table_store
            ),
        )
        catalog.add_datatable(
            self.output__classification_model_is_trained_on_cls_frozen_dataset,
            Table(
                ds.get_or_create_table(
                    self.output__classification_model_is_trained_on_cls_frozen_dataset,
                    TableStoreDB(
                        dbconn=ds.meta_dbconn,
                        name=self.output__classification_model_is_trained_on_cls_frozen_dataset,
                        data_sql_schema=classification_model_primary_columns
                        + [
                            Column(self.classification_frozen_dataset_id__name, String, primary_key=True),
                            Column("classification_train_config_id", String, primary_key=True),
                            Column("classification_train_config__params", JSON),
                        ],
                        create_table=self.create_table,
                    ),
                ).table_store
            ),
        )
        # ---
        pipeline = Pipeline(
            [
                BatchGenerate(
                    func=get_tf_classification_train_configs,
                    outputs=[self.output__tf_classification_train_config],
                    kwargs=dict(tf_classification_train_configs=self.tf_classification_train_configs),
                    labels=self.labels,
                ),
                DatatableBatchTransform(
                    func=train_tf_classification_model,
                    inputs=[
                        self.input__classification_frozen_dataset,
                        self.input__classification_frozen_dataset__has__image_gt,
                        self.output__tf_classification_train_config,
                    ],
                    outputs=[
                        self.output__classification_model,
                        self.output__classification_model_is_trained_on_cls_frozen_dataset,
                    ],
                    transform_keys=classification_model_other_primary_keys
                    + [self.classification_frozen_dataset_id__name, "classification_train_config_id"],
                    chunk_size=1,
                    kwargs=dict(
                        models_dir=str(Pathy.fluid(self.working_dir) / "models"),
                        max_within_time=self.max_within_time,
                        dt__classification_model=ds.get_table(self.output__classification_model),
                        dt__classification_model_is_trained_on_cls_frozen_dataset=ds.get_table(
                            self.output__classification_model_is_trained_on_cls_frozen_dataset
                        ),
                        classification_model_other_primary_keys=classification_model_other_primary_keys,
                        classification_model_id__name=self.classification_model_id__name,
                        classification_frozen_dataset_id__name=self.classification_frozen_dataset_id__name,
                        clean_checkpoints_after_train=self.clean_checkpoints_after_train,
                        tmp_folder=self.tmp_folder,
                        model_suffix=self.model_suffix,
                        training_launcher_config=self.training_launcher_config,
                    ),
                    labels=self.labels,
                ),
            ]
        )
        return build_compute(ds, catalog, pipeline)
