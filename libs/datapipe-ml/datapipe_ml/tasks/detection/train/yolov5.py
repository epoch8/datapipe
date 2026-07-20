# algos/yolov5.py
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from datapipe.compute import Catalog, ComputeStep, PipelineStep
from datapipe.datatable import DataStore, DataTable
from datapipe.executor import ExecutorConfig
from datapipe.run_config import RunConfig
from datapipe.types import IndexDF, Labels

from datapipe_ml.frameworks.yolo.checkpoint_label import build_yolo_train_config_summary
from datapipe_ml.training.request_runner import run_training_request
from datapipe_ml.training.train_config_id import train_configs_to_dataframe
from datapipe_ml.frameworks.yolo.datapipe_compute import (
    YoloModeSpec,
    build_yolo_compute,
)
from datapipe_ml.frameworks.yolo.dataset import (
    CustomYOLOLabelsFile,
    get_class_names_from_det_frozen_dataset_gt,
    resize_and_prepare_yolo_images,
)
from datapipe_ml.training.paths import default_tmp_folder, remote_input_path, remote_output_models_path
from datapipe_ml.training.specs import TrainingResumeCheckpoint
from datapipe_ml.frameworks.yolo.training import (
    YoloBaseAlgo,
    YoloPreparedData,
    YoloTrainContext,
    YoloTrainRuntimeConfig,
)

# Import concrete pieces for v5
from datapipe_ml.frameworks.yolo.yolov5.runner import YoloV5_TrainingConfig
from datapipe_ml.frameworks.yolo.yolov5.runner import YoloV5_TrainingConfig as _V5Config
from datapipe_ml.frameworks.yolo.yolov5.runner import train_process as _v5_train_process
from datapipe_ml.training.orchestrator import orchestrate
from datapipe_ml.training.specs import (
    PreparedData,
    TrainContext,
    TrainingLauncherConfig,
    TrainingLaunchRequest,
    TrainingPathMap,
    TrainingResumeConfig,
    TrainingSyncConfig,
    build_training_launcher,
)


class YoloV5DetectionAlgo(YoloBaseAlgo):
    train_config_id_col = "detection_train_config_id"
    train_params_col = "detection_train_config__params"
    frozen_created_at_col = "detection_frozen_dataset__created_at"
    images_count_col = "detection_frozen_dataset__images_count"
    model_row_prefix = "detection_model"

    type_name = "yolov5"
    TrainingConfigClass = _V5Config
    train_process_func = staticmethod(_v5_train_process)  # type: ignore[assignment]
    model_key = "weights"
    threshold_mode = "best_threshold"
    task = None  # v5 train_process has different signature

    def apply_resume_checkpoint(
        self,
        ctx: TrainContext,
        train_params: Dict[str, Any],
        checkpoint_path: Optional[str],
        checkpoint_epoch: Optional[int] = None,
    ) -> Dict[str, Any]:
        params = dict(train_params)
        if checkpoint_path is None:
            return params
        resolved_path = self._resolve_yolo_resume_checkpoint_path(checkpoint_path, checkpoint_epoch)
        params["resume"] = resolved_path
        params.pop("initial_weights_path", None)
        params["exist_ok"] = True
        return params

    def launch_training(
        self,
        ctx: TrainContext,
        idx: IndexDF,
        model_id: str,
        train_params: Dict[str, Any],
        data: PreparedData,
        resume_checkpoint: Optional[TrainingResumeCheckpoint] = None,
    ):
        assert isinstance(ctx, YoloTrainContext)
        assert isinstance(data, YoloPreparedData)
        if resume_checkpoint is not None:
            train_params = self.apply_resume_checkpoint(
                ctx,
                train_params,
                resume_checkpoint.path,
                resume_checkpoint.epoch,
            )
        yolov5_train_config = self._build_training_config(ctx, idx, model_id, train_params, data)
        subprocess_sync_config = None if ctx.training_output_write_dir else ctx.sync_config
        launcher = build_training_launcher(ctx.training_launcher_config)
        return launcher.launch(
            TrainingLaunchRequest.from_path_maps(
                target=_v5_train_process,
                args=(
                    yolov5_train_config,
                    data.objects_count,
                    data.class_names,
                    data.image_filepaths,
                    data.yolo_txt_filepaths,
                    subprocess_sync_config,
                ),
                cluster_suffix=model_id,
                inputs=(TrainingPathMap(data.data_src_path, remote_input_path("data")),),
                outputs=(TrainingPathMap(str(ctx.models_dir), remote_output_models_path()),),
            )
        )


def train_yolov5(
    ds,
    idx,
    input_dts: List[DataTable],
    run_config: Optional[RunConfig] = None,
    kwargs: Optional[Dict[str, Any]] = None,
) -> Tuple[pd.DataFrame, ...]:
    kwargs = kwargs or {}
    (
        dt__detection_frozen_dataset,
        dt__detection_training_request,
        dt__detection_frozen_dataset__class_names,
        dt__detection_frozen_dataset__resized_image_file,
        dt__detection_frozen_dataset__yolo_txt,
    ) = input_dts

    df_frozen_dataset = dt__detection_frozen_dataset.get_data(idx)
    df_training_request = dt__detection_training_request.get_data(idx)
    df_class_names = dt__detection_frozen_dataset__class_names.get_data(idx)
    df_resized_images = dt__detection_frozen_dataset__resized_image_file.get_data(idx)
    df_yolo_txt = dt__detection_frozen_dataset__yolo_txt.get_data(idx)

    def _train_callable(
        *,
        df_train_config: pd.DataFrame,
        max_within_time: Optional[str],
        force_training: bool,
        **_: Any,
    ) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        runtime = YoloTrainRuntimeConfig.from_kwargs(
            kwargs,
            model_table_key="dt__detection_model",
            link_table_key="dt__detection_model_is_trained_on_detection_frozen_dataset",
            model_other_primary_keys_key="detection_model_other_primary_keys",
            model_id_key="detection_model_id__name",
            frozen_dataset_id_key="detection_frozen_dataset_id__name",
        )
        ctx = runtime.build_context(
            YoloTrainContext,
            dt__frozen_dataset=dt__detection_frozen_dataset,
            dt__train_config=None,
            dt__class_names=dt__detection_frozen_dataset__class_names,
            dt__resized_image_file=dt__detection_frozen_dataset__resized_image_file,
            dt__yolo_txt=dt__detection_frozen_dataset__yolo_txt,
        )
        if max_within_time is not None:
            ctx = replace(ctx, max_within_time=max_within_time)
        algo = YoloV5DetectionAlgo()
        out = orchestrate(
            idx,
            ctx,
            algo,
            df_train_config=df_train_config,
            force_training=force_training,
        )
        return (out.df__model, out.df__link, out.df__training_status)

    return run_training_request(
        df_frozen_dataset=df_frozen_dataset,
        df_training_request=df_training_request,
        df_class_names=df_class_names,
        df_resized_images=df_resized_images,
        df_yolo_txt=df_yolo_txt,
        train_callable=_train_callable,
        train_config_id_col="detection_train_config_id",
        train_config_params_col="detection_train_config__params",
        dt_training_status=kwargs.get("dt__training_status"),
    )


def get_yolov5_detection_train_configs(yolov5_train_configs: List[Any]):
    yield train_configs_to_dataframe(
        yolov5_train_configs,
        id_column="detection_train_config_id",
        params_column="detection_train_config__params",
        summary_builder=lambda params: build_yolo_train_config_summary(
            params,
            model_key="weights",
            batch_key="batch_size",
        ),
        config_type="yolov5_detection",
    )


@dataclass
class Train_YoloV5_DetectionModel(PipelineStep):
    # --- keep shared fields unchanged ---
    input__detection_frozen_dataset: str
    input__detection_frozen_dataset__has__image_gt: str
    output__yolov5_train_config: str
    output__detection_training_request: str
    output__model_detection_size_for_resize: str
    output__detection_size_for_resize: str
    output__detection_frozen_dataset__class_names: str
    output__detection_frozen_dataset__resized_image_file: str
    output__detection_frozen_dataset__yolo_txt: str
    output__detection_model: str
    output__detection_model_is_trained_on_detection_frozen_dataset: str
    output__training_status: str
    working_dir: str
    yolov5_train_configs: List[YoloV5_TrainingConfig]
    primary_keys: List[str]
    max_within_time: str = "1w"
    bbox_id__name: Optional[str] = None
    image__image_path__name: str = "image__image_path"
    separator_to_split_attrnames: str = "__"
    create_table: bool = False
    labels: Optional[Labels] = None
    executor_config: Optional[ExecutorConfig] = None
    prepare_data_executor_config: Optional[ExecutorConfig] = None
    resize_images: bool = True
    detection_model_primary_keys: Optional[List[str]] = None
    detection_model_id__name: str = "detection_model_id"
    detection_frozen_dataset_id__name: str = "detection_frozen_dataset_id"
    tmp_folder: str = field(default_factory=default_tmp_folder)
    allow_sample_size_mismatch: bool = False
    model_suffix: str = "_default"
    training_launcher_config: Optional[TrainingLauncherConfig] = None
    sync_config: Optional[TrainingSyncConfig] = None
    resume_config: Optional[TrainingResumeConfig] = None
    filedir_fsspec_kwargs: dict[str, Any] | None = None

    def build_compute(self, ds: DataStore, catalog: Catalog) -> List[ComputeStep]:
        return build_yolo_compute(
            ds=ds,
            catalog=catalog,
            input__frozen_dataset=self.input__detection_frozen_dataset,
            input__frozen_dataset__has__image_gt=self.input__detection_frozen_dataset__has__image_gt,
            output__train_config=self.output__yolov5_train_config,
            output__training_request=self.output__detection_training_request,
            output__model_size_for_resize=self.output__model_detection_size_for_resize,
            output__size_for_resize=self.output__detection_size_for_resize,
            output__frozen_dataset__class_names=self.output__detection_frozen_dataset__class_names,
            output__frozen_dataset__resized_image_file=self.output__detection_frozen_dataset__resized_image_file,
            output__frozen_dataset__yolo_txt=self.output__detection_frozen_dataset__yolo_txt,
            output__model=self.output__detection_model,
            output__model_is_trained_on_frozen_dataset=self.output__detection_model_is_trained_on_detection_frozen_dataset,
            working_dir=self.working_dir,
            filedir_fsspec_kwargs=self.filedir_fsspec_kwargs,
            primary_keys=self.primary_keys,
            model_primary_keys=self.detection_model_primary_keys,
            model_id__name=self.detection_model_id__name,
            frozen_dataset_id__name=self.detection_frozen_dataset_id__name,
            image__image_path__name=self.image__image_path__name,
            bbox_id__name=self.bbox_id__name,
            separator_to_split_attrnames=self.separator_to_split_attrnames,
            create_table=self.create_table,
            labels=self.labels,
            executor_config=self.executor_config,
            prepare_data_executor_config=self.prepare_data_executor_config,
            resize_images=self.resize_images,
            max_within_time=self.max_within_time,
            tmp_folder=self.tmp_folder,
            model_suffix=self.model_suffix,
            allow_sample_size_mismatch=self.allow_sample_size_mismatch,
            mode=YoloModeSpec(
                fd_folder_name="detection_frozen_dataset",
                model_prefix="detection_model",
                train_config_id_col="detection_train_config_id",
                train_config_params_col="detection_train_config__params",
                labels_adapter_factory=CustomYOLOLabelsFile,
                get_train_configs_func=get_yolov5_detection_train_configs,
                resize_and_prepare_images_func=resize_and_prepare_yolo_images,
                get_class_names_from_gt_func=get_class_names_from_det_frozen_dataset_gt,
                train_callable=train_yolov5,
                models_subdir="models",
                train_config_type="yolov5_detection",
            ),
            train_configs_list=dict(yolov5_train_configs=self.yolov5_train_configs),
            training_launcher_config=self.training_launcher_config,
            sync_config=self.sync_config,
            resume_config=self.resume_config,
            output__training_status=self.output__training_status,
        )
