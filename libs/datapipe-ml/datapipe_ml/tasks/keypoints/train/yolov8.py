import logging
from dataclasses import dataclass, field
from typing import Any, List, Optional

from datapipe.compute import Catalog, ComputeStep, PipelineStep
from datapipe.datatable import DataStore
from datapipe.executor import ExecutorConfig
from datapipe.types import Labels
from sqlalchemy import Column, Float
from sqlalchemy.sql.sqltypes import JSON

from datapipe_ml.frameworks.yolo.dataset import (
    CustomYOLOV8PoseLabelsFile,
    get_class_names_from_kps_frozen_dataset_gt,
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

KEYPOINTS_YOLOV8_SPEC = YoloV8TaskSpec(
    task="pose",
    type_name="yolov8_pose",
    train_config_id_col="keypoints_train_config_id",
    train_params_col="keypoints_train_config__params",
    frozen_created_at_col="keypoints_frozen_dataset__created_at",
    images_count_col="keypoints_frozen_dataset__images_count",
    model_row_prefix="keypoints_model",
    metrics_mAP_05_col="metrics_mAP_0_5_pose",
    metrics_mAP_0595_col="metrics_mAP_0_5_to_0_95_pose",
    fd_folder_name="keypoints_frozen_dataset",
    models_subdir="keypoints_models",
    labels_adapter_factory=CustomYOLOV8PoseLabelsFile,
    get_class_names_from_gt_func=get_class_names_from_kps_frozen_dataset_gt,
    resize_and_prepare_images_func=resize_and_prepare_yolo_images,
    runtime_model_table_key="dt__keypoints_model",
    runtime_link_table_key="dt__keypoints_model_is_trained_on_keypoints_frozen_dataset",
    runtime_model_other_primary_keys_key="keypoints_model_other_primary_keys",
    runtime_model_id_key="keypoints_model_id__name",
    runtime_frozen_dataset_id_key="keypoints_frozen_dataset_id__name",
    extra_model_metric_map={
        "pose_P": "metrics_precision_pose",
        "pose_R": "metrics_recall_pose",
        "pose_mAP50": "metrics_mAP_0_5_pose",
        "pose_mAP50_95": "metrics_mAP_0_5_to_0_95_pose",
    },
    extra_class_names_columns=[
        Column("kpt_shape", JSON),
        Column("flip_idx", JSON),
    ],
    extra_model_columns=[
        Column("keypoints_model__pose_P", Float),
        Column("keypoints_model__pose_R", Float),
        Column("keypoints_model__pose_mAP50", Float),
        Column("keypoints_model__pose_mAP50_95", Float),
    ],
    extra_class_names_to_yaml_fields=["kpt_shape", "flip_idx"],
)

KEYPOINTS_YOLOV8_STEP_FIELDS = YoloV8TrainStepFields(
    input__frozen_dataset="input__keypoints_frozen_dataset",
    input__frozen_dataset__has__image_gt="input__keypoints_frozen_dataset__has__image_gt",
    output__train_config="output__yolov8_train_config",
    output__size_for_resize="output__keypoints_size_for_resize",
    output__frozen_dataset__class_names="output__keypoints_frozen_dataset__class_names",
    output__frozen_dataset__resized_image_file="output__keypoints_frozen_dataset__resized_image_file",
    output__frozen_dataset__yolo_txt="output__keypoints_frozen_dataset__yolo_txt",
    output__model="output__keypoints_model",
    output__model_is_trained_on_frozen_dataset="output__keypoints_model_is_trained_on_keypoints_frozen_dataset",
    output__training_status="output__training_status",
    model_primary_keys_attr="keypoints_model_primary_keys",
    model_id__name="keypoints_model_id__name",
    frozen_dataset_id__name="keypoints_frozen_dataset_id__name",
)


class YoloV8KeypointsAlgo(YoloBaseAlgo):
    train_config_id_col = "keypoints_train_config_id"
    train_params_col = "keypoints_train_config__params"
    frozen_created_at_col = "keypoints_frozen_dataset__created_at"
    images_count_col = "keypoints_frozen_dataset__images_count"
    model_row_prefix = "keypoints_model"
    type_name = "yolov8_pose"
    TrainingConfigClass = YoloV8_TrainingConfig
    train_process_func = staticmethod(_v8_train_process)  # type: ignore[assignment]
    model_key = "model"
    metrics_mAP_05_col = "metrics_mAP_0_5_pose"
    metrics_mAP_0595_col = "metrics_mAP_0_5_to_0_95_pose"
    threshold_mode = "best_threshold"
    task = "pose"
    extra_model_metric_map = {
        "pose_P": "metrics_precision_pose",
        "pose_R": "metrics_recall_pose",
        "pose_mAP50": "metrics_mAP_0_5_pose",
        "pose_mAP50_95": "metrics_mAP_0_5_to_0_95_pose",
    }


train_yolov8_keypoints = make_yolov8_train_callable(KEYPOINTS_YOLOV8_SPEC, YoloV8KeypointsAlgo)
get_yolov8_keypoints_train_configs = make_yolov8_get_train_configs(KEYPOINTS_YOLOV8_SPEC)

logger = logging.getLogger("datapipe.ml.yolov8.keypoints.script")


@dataclass
class Train_YoloV8_KeypointsModel(PipelineStep):
    input__keypoints_frozen_dataset: str
    input__keypoints_frozen_dataset__has__image_gt: str
    output__yolov8_train_config: str
    output__keypoints_size_for_resize: str
    output__keypoints_frozen_dataset__class_names: str
    output__keypoints_frozen_dataset__resized_image_file: str
    output__keypoints_frozen_dataset__yolo_txt: str
    output__keypoints_model: str
    output__keypoints_model_is_trained_on_keypoints_frozen_dataset: str
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
    keypoints_model_primary_keys: Optional[List[str]] = None
    keypoints_model_id__name: str = "keypoints_model_id"
    keypoints_frozen_dataset_id__name: str = "keypoints_frozen_dataset_id"
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
            spec=KEYPOINTS_YOLOV8_SPEC,
            fields=KEYPOINTS_YOLOV8_STEP_FIELDS,
            yolov8_train_configs=self.yolov8_train_configs,
            required_has_gt_columns=[
                "subset_id",
                self.image__image_path__name,
                "bboxes",
                "labels",
                "keypoints",
            ],
            train_callable=train_yolov8_keypoints,
        )
