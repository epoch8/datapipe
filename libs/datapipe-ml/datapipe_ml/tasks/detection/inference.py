import os
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import List, Optional, Tuple, Union

from cv_pipeliner import YOLOv5_ModelSpec
from cv_pipeliner.inferencers.detection.core import DetectionModelSpec
from cv_pipeliner.inferencers.detection.yolov8 import YOLOv8_ModelSpec
from datapipe.compute import Catalog, ComputeStep, PipelineStep
from datapipe.datatable import DataStore
from datapipe.executor import ExecutorConfig
from datapipe.run_config import LabelDict
from datapipe.types import Labels, PipelineInput, PipelineOutput

from datapipe_ml.inference.bbox_pipeline import (
    BboxInferenceStepConfig,
    BboxTaskInferenceSpec,
    InferenceMode,
    build_bbox_inference_compute,
    make_bbox_inference_func,
    make_bbox_inference_using_thresholds_func,
    run_bbox_task_inference,
)
from datapipe_ml.inference.common import min_prediction_threshold_from_class_thresholds

_MODEL_COLUMNS = [
    "detection_model__input_size",
    "detection_model__score_threshold",
    "detection_model__model_path",
    "detection_model__type",
    "detection_model__class_names",
]


def get_detection_model_spec(
    detection_model__input_size: Tuple[int, int],
    detection_model__model_path: str,
    detection_model__class_names: Tuple[str, ...],
    detection_model__type: str,
) -> DetectionModelSpec:
    if detection_model__type == "yolov5":
        return YOLOv5_ModelSpec(
            model_path=detection_model__model_path,
            class_names=detection_model__class_names,
            input_size=detection_model__input_size,
        )
    if detection_model__type == "yolov8":
        return YOLOv8_ModelSpec(
            model_path=detection_model__model_path,
            class_names=detection_model__class_names,
            input_size=detection_model__input_size,
        )
    raise ValueError(f"Unknown {detection_model__type=}")


def _get_detection_model_spec(
    *,
    input_size: Tuple[int, int],
    model_path: str,
    class_names: Tuple[str, ...],
    model_type: str,
) -> DetectionModelSpec:
    return get_detection_model_spec(
        detection_model__input_size=input_size,
        detection_model__model_path=model_path,
        detection_model__class_names=class_names,
        detection_model__type=model_type,
    )


DETECTION_INFERENCE_SPEC = BboxTaskInferenceSpec(
    model_prefix="detection_model",
    default_model_primary_keys=["detection_model_id"],
    model_columns=_MODEL_COLUMNS,
    get_model_spec=_get_detection_model_spec,
    model_primary_keys_kwarg="detection_model_primary_keys",
)


def detection_inference(*dfs, **kwargs):
    if "detection_model_primary_keys" in kwargs:
        kwargs["model_primary_keys"] = kwargs.pop("detection_model_primary_keys")
    return run_bbox_task_inference(DETECTION_INFERENCE_SPEC, *dfs, **kwargs)


detection_inference_by_crops = make_bbox_inference_func(DETECTION_INFERENCE_SPEC, inference_by_crops=True)
detection_inference_using_thresholds = make_bbox_inference_using_thresholds_func(
    DETECTION_INFERENCE_SPEC,
    make_bbox_inference_func(DETECTION_INFERENCE_SPEC),
)


@dataclass
class Inference_DetectionModel(PipelineStep):
    input__image: PipelineInput | Sequence[PipelineInput]
    input__detection_model: PipelineInput
    output__detection_prediction: PipelineOutput
    primary_keys: List[str]
    chunk_size: int = 64
    create_table: bool = False
    labels: Optional[Labels] = None
    image__image_path__name: str = "image__image_path"
    bbox_id__name: Optional[str] = "bbox_id"
    batch_size_default: int = int(os.environ.get("DETECTION_BATCH_SIZE_DEFAULT", 64))
    executor_config: Optional[ExecutorConfig] = None
    filters: Union[LabelDict, Callable[[], LabelDict], None] = None
    detection_model_primary_keys: Optional[List[str]] = None
    prediction_threshold: Optional[float] = None

    def build_compute(self, ds: DataStore, catalog: Catalog) -> List[ComputeStep]:
        return build_bbox_inference_compute(
            ds,
            catalog,
            BboxInferenceStepConfig(
                spec=DETECTION_INFERENCE_SPEC,
                mode=InferenceMode.PLAIN,
                input__image=self.input__image,
                input__model=self.input__detection_model,
                output__prediction=self.output__detection_prediction,
                primary_keys=self.primary_keys,
                chunk_size=self.chunk_size,
                create_table=self.create_table,
                labels=self.labels,
                image__image_path__name=self.image__image_path__name,
                bbox_id__name=self.bbox_id__name,
                batch_size_default=self.batch_size_default,
                executor_config=self.executor_config,
                filters=self.filters,
                model_primary_keys=self.detection_model_primary_keys,
                prediction_threshold=self.prediction_threshold,
            ),
        )


@dataclass
class InferenceBySplitOnCrops_DetectionModel(PipelineStep):
    input__image: PipelineInput | Sequence[PipelineInput]
    input__detection_model: PipelineInput
    output__detection_prediction: PipelineOutput
    primary_keys: List[str]
    hCrossing: int
    vCrossing: int
    thresholdSpace: int
    blockWidth: int
    blockHeight: int
    chunk_size: int = 64
    create_table: bool = False
    labels: Optional[Labels] = None
    image__image_path__name: str = "image__image_path"
    bbox_id__name: Optional[str] = "bbox_id"
    batch_size_default: int = int(os.environ.get("DETECTION_BATCH_SIZE_DEFAULT", 4))
    executor_config: Optional[ExecutorConfig] = None
    filters: Union[LabelDict, Callable[[], LabelDict], None] = None
    detection_model_primary_keys: Optional[List[str]] = None
    prediction_threshold: Optional[float] = None

    def build_compute(self, ds: DataStore, catalog: Catalog) -> List[ComputeStep]:
        return build_bbox_inference_compute(
            ds,
            catalog,
            BboxInferenceStepConfig(
                spec=DETECTION_INFERENCE_SPEC,
                mode=InferenceMode.CROPS,
                input__image=self.input__image,
                input__model=self.input__detection_model,
                output__prediction=self.output__detection_prediction,
                primary_keys=self.primary_keys,
                chunk_size=self.chunk_size,
                create_table=self.create_table,
                labels=self.labels,
                image__image_path__name=self.image__image_path__name,
                bbox_id__name=self.bbox_id__name,
                batch_size_default=self.batch_size_default,
                executor_config=self.executor_config,
                filters=self.filters,
                model_primary_keys=self.detection_model_primary_keys,
                prediction_threshold=self.prediction_threshold,
                hCrossing=self.hCrossing,
                vCrossing=self.vCrossing,
                thresholdSpace=self.thresholdSpace,
                blockWidth=self.blockWidth,
                blockHeight=self.blockHeight,
            ),
        )


@dataclass
class Inference_UsingThresholdsPerClasss_DetectionModel(PipelineStep):
    input__image: PipelineInput | Sequence[PipelineInput]
    input__detection_model: PipelineInput
    input__detection_model_thresholds: PipelineInput
    output__detection_prediction: PipelineOutput
    primary_keys: List[str]
    chunk_size: int = 64
    create_table: bool = False
    labels: Optional[Labels] = None
    image__image_path__name: str = "image__image_path"
    bbox_id__name: Optional[str] = "bbox_id"
    batch_size_default: int = int(os.environ.get("DETECTION_BATCH_SIZE_DEFAULT", 64))
    executor_config: Optional[ExecutorConfig] = None
    filters: Union[LabelDict, Callable[[], LabelDict], None] = None
    detection_model_primary_keys: Optional[List[str]] = None
    class_name_to_threshold__name: str = "class_name_to_threshold"

    def build_compute(self, ds: DataStore, catalog: Catalog) -> List[ComputeStep]:
        return build_bbox_inference_compute(
            ds,
            catalog,
            BboxInferenceStepConfig(
                spec=DETECTION_INFERENCE_SPEC,
                mode=InferenceMode.THRESHOLDS,
                input__image=self.input__image,
                input__model=self.input__detection_model,
                input__model_thresholds=self.input__detection_model_thresholds,
                output__prediction=self.output__detection_prediction,
                primary_keys=self.primary_keys,
                chunk_size=self.chunk_size,
                create_table=self.create_table,
                labels=self.labels,
                image__image_path__name=self.image__image_path__name,
                bbox_id__name=self.bbox_id__name,
                batch_size_default=self.batch_size_default,
                executor_config=self.executor_config,
                filters=self.filters,
                model_primary_keys=self.detection_model_primary_keys,
                class_name_to_threshold__name=self.class_name_to_threshold__name,
            ),
        )


Inference_UsingThresholdsPerClass_DetectionModel = Inference_UsingThresholdsPerClasss_DetectionModel
