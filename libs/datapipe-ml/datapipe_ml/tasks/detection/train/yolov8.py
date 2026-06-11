from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import List, Optional

from datapipe.compute import Catalog, ComputeStep, PipelineStep
from datapipe.datatable import DataStore
from datapipe.executor import ExecutorConfig
from datapipe.types import Labels

from datapipe_ml.frameworks.yolo.checkpoint_label import build_yolo_train_config_summary
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
from datapipe_ml.frameworks.yolo.training import (
    YoloBaseAlgo,
    YoloTrainContext,
    YoloTrainRuntimeConfig,
)
from datapipe_ml.frameworks.yolo.yolov8.runner import YoloV8_TrainingConfig
from datapipe_ml.frameworks.yolo.yolov8.runner import YoloV8_TrainingConfig as _V8Config
from datapipe_ml.frameworks.yolo.yolov8.runner import train_process as _v8_train_process
from datapipe_ml.training.orchestrator import orchestrate
from datapipe_ml.training.specs import TrainingLauncherConfig, TrainingResumeConfig, TrainingSyncConfig


class YoloV8DetectionAlgo(YoloBaseAlgo):
    train_config_id_col = "detection_train_config_id"
    train_params_col = "detection_train_config__params"
    frozen_created_at_col = "detection_frozen_dataset__created_at"
    images_count_col = "detection_frozen_dataset__images_count"
    model_row_prefix = "detection_model"

    type_name = "yolov8"
    TrainingConfigClass = _V8Config
    train_process_func = staticmethod(_v8_train_process)  # type: ignore[assignment]
    model_key = "model"
    threshold_mode = "best_threshold"
    task = "detect"


logger = logging.getLogger("datapipe.ml.yolov8.script")


def train_yolov8(ds, idx, input_dts, run_config=None, kwargs=None):
    kwargs = kwargs or {}
    dt__frozen_dataset, dt__train_config, dt__class_names, dt__resized_image_file, dt__yolo_txt = input_dts
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
        dt__frozen_dataset=dt__frozen_dataset,
        dt__train_config=dt__train_config,
        dt__class_names=dt__class_names,
        dt__resized_image_file=dt__resized_image_file,
        dt__yolo_txt=dt__yolo_txt,
    )
    algo = YoloV8DetectionAlgo()
    out = orchestrate(idx, ctx, algo)
    return (out.df__model, out.df__link, out.df__training_status)


def get_yolov8_detection_train_configs(yolov8_train_configs: List[YoloV8_TrainingConfig]):
    yield train_configs_to_dataframe(
        yolov8_train_configs,
        id_column="detection_train_config_id",
        params_column="detection_train_config__params",
        summary_builder=build_yolo_train_config_summary,
    )


@dataclass
class Train_YoloV8_DetectionModel(PipelineStep):
    # те же поля, что и раньше…
    input__detection_frozen_dataset: str
    input__detection_frozen_dataset__has__image_gt: str
    output__yolov8_train_config: str
    output__detection_size_for_resize: str
    output__detection_frozen_dataset__class_names: str
    output__detection_frozen_dataset__resized_image_file: str
    output__detection_frozen_dataset__yolo_txt: str
    output__detection_model: str
    output__detection_model_is_trained_on_detection_frozen_dataset: str
    output__training_status: str
    working_dir: str
    yolov8_train_configs: List[YoloV8_TrainingConfig]
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
    tmp_folder: str = "/tmp/"
    ignore_errors_sample_sizes: bool = False
    model_suffix: str = "_default"
    training_launcher_config: Optional[TrainingLauncherConfig] = None
    sync_config: Optional[TrainingSyncConfig] = None
    resume_config: Optional[TrainingResumeConfig] = None

    def build_compute(self, ds: DataStore, catalog: Catalog) -> List[ComputeStep]:
        return build_yolo_compute(
            ds=ds,
            catalog=catalog,
            input__frozen_dataset=self.input__detection_frozen_dataset,
            input__frozen_dataset__has__image_gt=self.input__detection_frozen_dataset__has__image_gt,
            output__train_config=self.output__yolov8_train_config,
            output__size_for_resize=self.output__detection_size_for_resize,
            output__frozen_dataset__class_names=self.output__detection_frozen_dataset__class_names,
            output__frozen_dataset__resized_image_file=self.output__detection_frozen_dataset__resized_image_file,
            output__frozen_dataset__yolo_txt=self.output__detection_frozen_dataset__yolo_txt,
            output__model=self.output__detection_model,
            output__model_is_trained_on_frozen_dataset=self.output__detection_model_is_trained_on_detection_frozen_dataset,
            working_dir=self.working_dir,
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
            ignore_errors_sample_sizes=self.ignore_errors_sample_sizes,
            mode=YoloModeSpec(
                fd_folder_name="detection_frozen_dataset",
                model_prefix="detection_model",
                train_config_id_col="detection_train_config_id",
                train_config_params_col="detection_train_config__params",
                labels_adapter_factory=CustomYOLOLabelsFile,
                get_train_configs_func=get_yolov8_detection_train_configs,
                resize_and_prepare_images_func=resize_and_prepare_yolo_images,
                get_class_names_from_gt_func=get_class_names_from_det_frozen_dataset_gt,
                train_callable=train_yolov8,
                models_subdir="models",
            ),
            train_configs_list=dict(yolov8_train_configs=self.yolov8_train_configs),
            training_launcher_config=self.training_launcher_config,
            sync_config=self.sync_config,
            resume_config=self.resume_config,
            output__training_status=self.output__training_status,
        )
