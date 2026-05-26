# algos/yolov5.py
from dataclasses import asdict, dataclass
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from datapipe.compute import Catalog, ComputeStep, PipelineStep
from datapipe.datatable import DataStore, DataTable
from datapipe.executor import ExecutorConfig
from datapipe.run_config import RunConfig
from datapipe.types import IndexDF, Labels

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
    build_training_launcher,
)


def resolve_yolov5_train_script(yolov5_script_file: Optional[str]) -> Optional[str]:
    if yolov5_script_file is not None:
        from pathlib import Path

        script_path = Path(yolov5_script_file)
        if not script_path.exists():
            raise ValueError(f"Train script yolov5 not found at {yolov5_script_file}.")
        return str(script_path)
    return yolov5_script_file


@dataclass
class YoloV5TrainContext(YoloTrainContext):
    yolov5_script_file: Optional[str] = None


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

    # v5 has different train_process signature: add yolov5_script_file
    def launch_training(
        self, ctx: TrainContext, idx: IndexDF, model_id: str, train_params: Dict[str, Any], data: PreparedData
    ):
        assert isinstance(ctx, YoloV5TrainContext)
        assert isinstance(data, YoloPreparedData)
        yolov5_train_config = self._build_training_config(ctx, idx, model_id, train_params, data)
        launcher = build_training_launcher(ctx.training_launcher_config)
        return launcher.launch(
            TrainingLaunchRequest.from_path_maps(
                target=_v5_train_process,
                args=(
                    yolov5_train_config,
                    resolve_yolov5_train_script(ctx.yolov5_script_file),
                    data.objects_count,
                    data.class_names,
                    data.image_filepaths,
                    data.yolo_txt_filepaths,
                    ctx.save_checkpoints_to_cloud,
                ),
                cluster_suffix=model_id,
                inputs=(TrainingPathMap(data.data_src_path, "/workspace/datapipe_ml/input/data"),),
                outputs=(TrainingPathMap(str(ctx.models_dir), "/workspace/datapipe_ml/output/models"),),
            )
        )


def train_yolov5(
    ds,
    idx,
    input_dts: List[DataTable],
    run_config: Optional[RunConfig] = None,
    kwargs: Optional[Dict[str, Any]] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    kwargs = kwargs or {}
    (
        dt__detection_frozen_dataset,
        dt__yolov5_train_config,
        dt__detection_frozen_dataset__class_names,
        dt__detection_frozen_dataset__resized_image_file,
        dt__detection_frozen_dataset__yolo_txt,
    ) = input_dts

    runtime = YoloTrainRuntimeConfig.from_kwargs(
        kwargs,
        model_table_key="dt__detection_model",
        link_table_key="dt__detection_model_is_trained_on_detection_frozen_dataset",
        model_other_primary_keys_key="detection_model_other_primary_keys",
        model_id_key="detection_model_id__name",
        frozen_dataset_id_key="detection_frozen_dataset_id__name",
    )
    ctx = runtime.build_context(
        YoloV5TrainContext,
        dt__frozen_dataset=dt__detection_frozen_dataset,
        dt__train_config=dt__yolov5_train_config,
        dt__class_names=dt__detection_frozen_dataset__class_names,
        dt__resized_image_file=dt__detection_frozen_dataset__resized_image_file,
        dt__yolo_txt=dt__detection_frozen_dataset__yolo_txt,
        yolov5_script_file=kwargs["yolov5_script_file"],
    )
    algo = YoloV5DetectionAlgo()
    out = orchestrate(idx, ctx, algo)
    return (out.df__model, out.df__link)


def get_yolov5_detection_train_configs(yolov5_train_configs: List[YoloV5_TrainingConfig]):
    yield pd.DataFrame(
        [
            dict(
                detection_train_config_id=(
                    f"{x.weights.replace('.pt', '')}-{x.imgsz}-default-batch{x.batch_size}-epochs{x.epochs}"
                    + (f"-from-ckpt-{x.initial_weights_path}" if x.initial_weights_path is not None else "")
                ),
                detection_train_config__params=asdict(x),
            )
            for x in yolov5_train_configs
        ]
    )


@dataclass
class Train_YoloV5_DetectionModel(PipelineStep):
    # --- общие поля оставляем как есть ---
    input__detection_frozen_dataset: str
    input__detection_frozen_dataset__has__image_gt: str
    output__yolov5_train_config: str
    output__detection_size_for_resize: str
    output__detection_frozen_dataset__class_names: str
    output__detection_frozen_dataset__resized_image_file: str
    output__detection_frozen_dataset__yolo_txt: str
    output__detection_model: str
    output__detection_model_is_trained_on_detection_frozen_dataset: str
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
    yolov5_script_file: Optional[str] = None
    resize_images: bool = True
    save_checkpoints_to_cloud: bool = False
    detection_model_primary_keys: Optional[List[str]] = None
    detection_model_id__name: str = "detection_model_id"
    detection_frozen_dataset_id__name: str = "detection_frozen_dataset_id"
    tmp_folder: str = "/tmp/"
    ignore_errors_sample_sizes: bool = False
    model_suffix: str = "_default"
    training_launcher_config: Optional[TrainingLauncherConfig] = None

    def build_compute(self, ds: DataStore, catalog: Catalog) -> List[ComputeStep]:
        return build_yolo_compute(
            ds=ds,
            catalog=catalog,
            input__frozen_dataset=self.input__detection_frozen_dataset,
            input__frozen_dataset__has__image_gt=self.input__detection_frozen_dataset__has__image_gt,
            output__train_config=self.output__yolov5_train_config,
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
            save_checkpoints_to_cloud=self.save_checkpoints_to_cloud,
            tmp_folder=self.tmp_folder,
            model_suffix=self.model_suffix,
            ignore_errors_sample_sizes=self.ignore_errors_sample_sizes,
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
            ),
            train_configs_list=dict(yolov5_train_configs=self.yolov5_train_configs),
            training_launcher_config=self.training_launcher_config,
            extra_train_kwargs=dict(yolov5_script_file=self.yolov5_script_file),
        )
