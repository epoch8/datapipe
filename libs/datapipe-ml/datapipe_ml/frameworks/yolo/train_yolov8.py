from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Literal, Optional, Protocol, Type

import pandas as pd
from datapipe.compute import Catalog, ComputeStep
from datapipe.datatable import DataStore, DataTable
from datapipe.executor import ExecutorConfig
from datapipe.run_config import RunConfig
from datapipe.types import IndexDF, Labels
from sqlalchemy import Column

from datapipe_ml.frameworks.yolo.checkpoint_label import build_yolo_train_config_summary
from datapipe_ml.frameworks.yolo.datapipe_compute import YoloModeSpec, build_yolo_compute
from datapipe_ml.frameworks.yolo.training import YoloBaseAlgo, YoloTrainContext, YoloTrainRuntimeConfig
from datapipe_ml.frameworks.yolo.yolov8.runner import YoloV8_TrainingConfig, train_process as _v8_train_process
from datapipe_ml.training.orchestrator import orchestrate
from datapipe_ml.training.paths import default_tmp_folder
from datapipe_ml.training.specs import TrainingLauncherConfig, TrainingResumeConfig, TrainingSyncConfig
from datapipe_ml.training.train_config_id import train_configs_to_dataframe


@dataclass(frozen=True)
class YoloV8TaskSpec:
    task: Literal["detect", "segment", "pose"]
    type_name: str
    train_config_id_col: str
    train_params_col: str
    frozen_created_at_col: str
    images_count_col: str
    model_row_prefix: str
    metrics_mAP_05_col: str
    metrics_mAP_0595_col: str
    fd_folder_name: str
    models_subdir: str
    labels_adapter_factory: Type
    get_class_names_from_gt_func: Callable[..., pd.DataFrame]
    resize_and_prepare_images_func: Callable[..., Any]
    runtime_model_table_key: str
    runtime_link_table_key: str
    runtime_model_other_primary_keys_key: str
    runtime_model_id_key: str
    runtime_frozen_dataset_id_key: str
    extra_model_metric_map: Dict[str, str] = field(default_factory=dict)
    extra_class_names_columns: List[Column] = field(default_factory=list)
    extra_model_columns: List[Column] = field(default_factory=list)
    extra_class_names_to_yaml_fields: List[str] = field(default_factory=list)
    required_has_gt_columns: Optional[List[str]] = None


def make_yolov8_train_callable(
    spec: YoloV8TaskSpec,
    algo_cls: Type[YoloBaseAlgo],
) -> Callable[..., tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]]:
    def train_yolov8(
        ds: DataStore,
        idx: IndexDF,
        input_dts: List[DataTable],
        run_config: Optional[RunConfig] = None,
        kwargs: Optional[Dict[str, Any]] = None,
    ) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        kwargs = kwargs or {}
        dt__frozen_dataset, dt__train_config, dt__class_names, dt__resized_image_file, dt__yolo_txt = input_dts
        runtime = YoloTrainRuntimeConfig.from_kwargs(
            kwargs,
            model_table_key=spec.runtime_model_table_key,
            link_table_key=spec.runtime_link_table_key,
            model_other_primary_keys_key=spec.runtime_model_other_primary_keys_key,
            model_id_key=spec.runtime_model_id_key,
            frozen_dataset_id_key=spec.runtime_frozen_dataset_id_key,
        )
        ctx = runtime.build_context(
            YoloTrainContext,
            dt__frozen_dataset=dt__frozen_dataset,
            dt__train_config=dt__train_config,
            dt__class_names=dt__class_names,
            dt__resized_image_file=dt__resized_image_file,
            dt__yolo_txt=dt__yolo_txt,
        )
        out = orchestrate(idx, ctx, algo_cls())
        return out.df__model, out.df__link, out.df__training_status

    return train_yolov8


def make_yolov8_get_train_configs(spec: YoloV8TaskSpec) -> Callable[[List[YoloV8_TrainingConfig]], Any]:
    def get_configs(yolov8_train_configs: List[YoloV8_TrainingConfig]):
        yield train_configs_to_dataframe(
            yolov8_train_configs,
            id_column=spec.train_config_id_col,
            params_column=spec.train_params_col,
            summary_builder=build_yolo_train_config_summary,
        )

    return get_configs


@dataclass(frozen=True)
class YoloV8TrainStepFields:
    input__frozen_dataset: str
    input__frozen_dataset__has__image_gt: str
    output__train_config: str
    output__size_for_resize: str
    output__frozen_dataset__class_names: str
    output__frozen_dataset__resized_image_file: str
    output__frozen_dataset__yolo_txt: str
    output__model: str
    output__model_is_trained_on_frozen_dataset: str
    output__training_status: str
    model_primary_keys_attr: str
    model_id__name: str
    frozen_dataset_id__name: str

    def model_primary_keys(self, step: YoloV8TrainStepLike) -> list[str] | None:
        value = getattr(step, self.model_primary_keys_attr)
        if value is None:
            return None
        if not isinstance(value, list):
            raise TypeError(
                f"Expected {self.model_primary_keys_attr!r} to be list[str] | None, got {type(value)!r}"
            )
        return value

    def model_id_column(self, step: YoloV8TrainStepLike) -> str:
        return _step_pipeline_io(step, self.model_id__name)

    def frozen_dataset_id_column(self, step: YoloV8TrainStepLike) -> str:
        return _step_pipeline_io(step, self.frozen_dataset_id__name)


class YoloV8TrainStepLike(Protocol):
    working_dir: str
    primary_keys: list[str]
    image__image_path__name: str
    bbox_id__name: str | None
    separator_to_split_attrnames: str
    create_table: bool
    labels: Labels | None
    executor_config: ExecutorConfig | None
    prepare_data_executor_config: ExecutorConfig | None
    resize_images: bool
    max_within_time: str
    tmp_folder: str
    model_suffix: str
    allow_sample_size_mismatch: bool
    training_launcher_config: TrainingLauncherConfig | None
    sync_config: TrainingSyncConfig | None
    resume_config: TrainingResumeConfig | None
    filedir_fsspec_kwargs: dict[str, Any] | None


def _step_pipeline_io(step: YoloV8TrainStepLike, attr_name: str) -> str:
    value = getattr(step, attr_name)
    if not isinstance(value, str):
        raise TypeError(f"Expected pipeline I/O attribute {attr_name!r} to be str, got {type(value)!r}")
    return value


def build_yolov8_train_compute(
    *,
    ds: DataStore,
    catalog: Catalog,
    step: YoloV8TrainStepLike,
    spec: YoloV8TaskSpec,
    fields: YoloV8TrainStepFields,
    yolov8_train_configs: List[YoloV8_TrainingConfig],
    train_callable: Callable[..., tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]],
    required_has_gt_columns: Optional[List[str]] = None,
) -> List[ComputeStep]:
    mode_kwargs: Dict[str, Any] = dict(
        fd_folder_name=spec.fd_folder_name,
        model_prefix=spec.model_row_prefix,
        train_config_id_col=spec.train_config_id_col,
        train_config_params_col=spec.train_params_col,
        labels_adapter_factory=spec.labels_adapter_factory,
        get_train_configs_func=make_yolov8_get_train_configs(spec),
        resize_and_prepare_images_func=spec.resize_and_prepare_images_func,
        get_class_names_from_gt_func=spec.get_class_names_from_gt_func,
        train_callable=train_callable,
        models_subdir=spec.models_subdir,
    )
    if spec.extra_class_names_columns:
        mode_kwargs["extra_class_names_columns"] = spec.extra_class_names_columns
    if spec.extra_model_columns:
        mode_kwargs["extra_model_columns"] = spec.extra_model_columns
    if spec.extra_class_names_to_yaml_fields:
        mode_kwargs["extra_class_names_to_yaml_fields"] = spec.extra_class_names_to_yaml_fields
    if required_has_gt_columns is not None:
        mode_kwargs["required_has_gt_columns"] = required_has_gt_columns
    elif spec.required_has_gt_columns is not None:
        mode_kwargs["required_has_gt_columns"] = spec.required_has_gt_columns

    return build_yolo_compute(
        ds=ds,
        catalog=catalog,
        input__frozen_dataset=_step_pipeline_io(step, fields.input__frozen_dataset),
        input__frozen_dataset__has__image_gt=_step_pipeline_io(step, fields.input__frozen_dataset__has__image_gt),
        output__train_config=_step_pipeline_io(step, fields.output__train_config),
        output__size_for_resize=_step_pipeline_io(step, fields.output__size_for_resize),
        output__frozen_dataset__class_names=_step_pipeline_io(step, fields.output__frozen_dataset__class_names),
        output__frozen_dataset__resized_image_file=_step_pipeline_io(
            step, fields.output__frozen_dataset__resized_image_file
        ),
        output__frozen_dataset__yolo_txt=_step_pipeline_io(step, fields.output__frozen_dataset__yolo_txt),
        output__model=_step_pipeline_io(step, fields.output__model),
        output__model_is_trained_on_frozen_dataset=_step_pipeline_io(
            step, fields.output__model_is_trained_on_frozen_dataset
        ),
        working_dir=step.working_dir,
        filedir_fsspec_kwargs=step.filedir_fsspec_kwargs,
        primary_keys=step.primary_keys,
        model_primary_keys=fields.model_primary_keys(step),
        model_id__name=fields.model_id_column(step),
        frozen_dataset_id__name=fields.frozen_dataset_id_column(step),
        image__image_path__name=step.image__image_path__name,
        bbox_id__name=step.bbox_id__name,
        separator_to_split_attrnames=step.separator_to_split_attrnames,
        create_table=step.create_table,
        labels=step.labels,
        executor_config=step.executor_config,
        prepare_data_executor_config=step.prepare_data_executor_config,
        resize_images=step.resize_images,
        max_within_time=step.max_within_time,
        tmp_folder=step.tmp_folder,
        model_suffix=step.model_suffix,
        allow_sample_size_mismatch=step.allow_sample_size_mismatch,
        mode=YoloModeSpec(**mode_kwargs),
        train_configs_list=dict(yolov8_train_configs=yolov8_train_configs),
        training_launcher_config=step.training_launcher_config,
        sync_config=step.sync_config,
        resume_config=step.resume_config,
        output__training_status=_step_pipeline_io(step, fields.output__training_status),
    )
