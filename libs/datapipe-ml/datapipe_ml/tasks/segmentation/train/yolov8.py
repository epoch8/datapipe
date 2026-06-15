import logging
from dataclasses import dataclass, field
from typing import Any, List, Optional

from datapipe.compute import Catalog, ComputeStep, PipelineStep
from datapipe.datatable import DataStore
from datapipe.executor import ExecutorConfig
from datapipe.types import Labels

from datapipe_ml.frameworks.yolo.dataset import (
    CustomYOLOV8SegmentatorLabelsFile,
    get_class_names_from_det_frozen_dataset_gt as get_class_names_from_segm_frozen_dataset_gt,
    resize_and_prepare_yolo_images,
)
from datapipe_ml.frameworks.yolo.train_yolov8 import (
    YoloV8TaskSpec,
    YoloV8TrainStepFields,
    build_yolov8_train_compute,
    make_yolov8_get_train_configs,
    make_yolov8_train_callable,
)
from datapipe_ml.frameworks.yolo.training import YoloBaseAlgo
from datapipe_ml.frameworks.yolo.yolov8.runner import YoloV8_TrainingConfig, train_process as _v8_train_process
from datapipe_ml.training.paths import default_tmp_folder
from datapipe_ml.training.specs import TrainingLauncherConfig, TrainingResumeConfig, TrainingSyncConfig

SEGMENTATION_YOLOV8_SPEC = YoloV8TaskSpec(
    task="segment",
    type_name="yolov8",
    train_config_id_col="segmentation_train_config_id",
    train_params_col="segmentation_train_config__params",
    frozen_created_at_col="segmentation_frozen_dataset__created_at",
    images_count_col="segmentation_frozen_dataset__images_count",
    model_row_prefix="segmentation_model",
    metrics_mAP_05_col="metrics_mAP_0_5_mask",
    metrics_mAP_0595_col="metrics_mAP_0_5_to_0_95_mask",
    fd_folder_name="segmentation_frozen_dataset",
    models_subdir="segmentation_models",
    labels_adapter_factory=CustomYOLOV8SegmentatorLabelsFile,
    get_class_names_from_gt_func=get_class_names_from_segm_frozen_dataset_gt,
    resize_and_prepare_images_func=resize_and_prepare_yolo_images,
    runtime_model_table_key="dt__segmentation_model",
    runtime_link_table_key="dt__segm_model_is_trained_on_segm_frozen_dataset",
    runtime_model_other_primary_keys_key="segmentation_model_other_primary_keys",
    runtime_model_id_key="segmentation_model_id__name",
    runtime_frozen_dataset_id_key="segmentation_frozen_dataset_id__name",
)

SEGMENTATION_YOLOV8_STEP_FIELDS = YoloV8TrainStepFields(
    input__frozen_dataset="input__segmentation_frozen_dataset",
    input__frozen_dataset__has__image_gt="input__segmentation_frozen_dataset__has__image_gt",
    output__train_config="output__yolov8_train_config",
    output__size_for_resize="output__segmentation_size_for_resize",
    output__frozen_dataset__class_names="output__segmentation_frozen_dataset__class_names",
    output__frozen_dataset__resized_image_file="output__segmentation_frozen_dataset__resized_image_file",
    output__frozen_dataset__yolo_txt="output__segmentation_frozen_dataset__yolo_txt",
    output__model="output__segmentation_model",
    output__model_is_trained_on_frozen_dataset="output__segm_model_is_trained_on_segm_frozen_dataset",
    output__training_status="output__training_status",
    model_primary_keys_attr="segmentation_model_primary_keys",
    model_id__name="segmentation_model_id__name",
    frozen_dataset_id__name="segmentation_frozen_dataset_id__name",
)


class YoloV8SegmentationAlgo(YoloBaseAlgo):
    train_config_id_col = "segmentation_train_config_id"
    train_params_col = "segmentation_train_config__params"
    frozen_created_at_col = "segmentation_frozen_dataset__created_at"
    images_count_col = "segmentation_frozen_dataset__images_count"
    model_row_prefix = "segmentation_model"
    type_name = "yolov8"
    TrainingConfigClass = YoloV8_TrainingConfig
    train_process_func = staticmethod(_v8_train_process)  # type: ignore[assignment]
    model_key = "model"
    metrics_mAP_05_col = "metrics_mAP_0_5_mask"
    metrics_mAP_0595_col = "metrics_mAP_0_5_to_0_95_mask"
    threshold_mode = "best_threshold"
    task = "segment"
    extra_model_metric_map: dict = {}


train_yolov8_segmentation = make_yolov8_train_callable(SEGMENTATION_YOLOV8_SPEC, YoloV8SegmentationAlgo)
get_yolov8_segmentation_train_configs = make_yolov8_get_train_configs(SEGMENTATION_YOLOV8_SPEC)

logger = logging.getLogger("datapipe.ml.yolov8.script")


@dataclass
class Train_YoloV8_SegmentationModel(PipelineStep):
    input__segmentation_frozen_dataset: str
    input__segmentation_frozen_dataset__has__image_gt: str
    output__yolov8_train_config: str
    output__segmentation_size_for_resize: str
    output__segmentation_frozen_dataset__class_names: str
    output__segmentation_frozen_dataset__resized_image_file: str
    output__segmentation_frozen_dataset__yolo_txt: str
    output__segmentation_model: str
    output__segm_model_is_trained_on_segm_frozen_dataset: str
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
    segmentation_model_primary_keys: Optional[List[str]] = None
    segmentation_model_id__name: str = "segmentation_model_id"
    segmentation_frozen_dataset_id__name: str = "segmentation_frozen_dataset_id"
    tmp_folder: str = field(default_factory=default_tmp_folder)
    ignore_errors_sample_sizes: bool = False
    model_suffix: str = "_default"
    training_launcher_config: Optional[TrainingLauncherConfig] = None
    sync_config: Optional[TrainingSyncConfig] = None
    resume_config: Optional[TrainingResumeConfig] = None
    filedir_fsspec_kwargs: dict[str, Any] | None = None

    def build_compute(self, ds: DataStore, catalog: Catalog) -> List[ComputeStep]:
        return build_yolov8_train_compute(
            ds=ds,
            catalog=catalog,
            step=self,
            spec=SEGMENTATION_YOLOV8_SPEC,
            fields=SEGMENTATION_YOLOV8_STEP_FIELDS,
            yolov8_train_configs=self.yolov8_train_configs,
            train_callable=train_yolov8_segmentation,
        )
