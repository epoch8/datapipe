import os
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import List, Optional, Tuple, Union

from cv_pipeliner.inferencers.detection.core import DetectionModelSpec
from cv_pipeliner.inferencers.detection.yolov8 import YOLOv8_ModelSpec
from datapipe.compute import Catalog, ComputeStep, PipelineStep
from datapipe.datatable import DataStore
from datapipe.executor import ExecutorConfig
from datapipe.run_config import LabelDict
from datapipe.types import Labels, PipelineInput, PipelineOutput
from sqlalchemy import Column
from sqlalchemy.sql.sqltypes import JSON

from datapipe_ml.inference.bbox_pipeline import (
    BboxInferenceStepConfig,
    BboxTaskInferenceSpec,
    InferenceMode,
    build_bbox_inference_compute,
    make_bbox_inference_func,
    make_bbox_inference_using_thresholds_func,
    run_bbox_task_inference,
)

_MODEL_COLUMNS = [
    "segmentation_model__input_size",
    "segmentation_model__score_threshold",
    "segmentation_model__model_path",
    "segmentation_model__type",
    "segmentation_model__class_names",
]


def get_segmentation_model_spec(
    segmentation_model__input_size: Tuple[int, int],
    segmentation_model__model_path: str,
    segmentation_model__class_names: Tuple[str, ...],
    segmentation_model__type: str,
) -> DetectionModelSpec:
    if segmentation_model__type == "yolov8":
        return YOLOv8_ModelSpec(
            model_path=segmentation_model__model_path,
            class_names=segmentation_model__class_names,
            input_size=segmentation_model__input_size,
        )
    raise ValueError(f"Unknown {segmentation_model__type=}")


def _get_segmentation_model_spec(
    *,
    input_size: Tuple[int, int],
    model_path: str,
    class_names: Tuple[str, ...],
    model_type: str,
) -> DetectionModelSpec:
    return get_segmentation_model_spec(
        segmentation_model__input_size=input_size,
        segmentation_model__model_path=model_path,
        segmentation_model__class_names=class_names,
        segmentation_model__type=model_type,
    )


SEGMENTATION_INFERENCE_SPEC = BboxTaskInferenceSpec(
    model_prefix="segmentation_model",
    default_model_primary_keys=["segmentation_model_id"],
    model_columns=_MODEL_COLUMNS,
    get_model_spec=_get_segmentation_model_spec,
    model_primary_keys_kwarg="segmentation_model_primary_keys",
    crop_include_masks=True,
    extra_bbox_id_after_coords=(Column("mask", JSON),),
    extra_json_after_labels=(Column("masks", JSON),),
)


def segmentation_inference(*dfs, **kwargs):
    if "segmentation_model_primary_keys" in kwargs:
        kwargs["model_primary_keys"] = kwargs.pop("segmentation_model_primary_keys")
    return run_bbox_task_inference(SEGMENTATION_INFERENCE_SPEC, *dfs, **kwargs)


segmentation_inference_by_crops = make_bbox_inference_func(SEGMENTATION_INFERENCE_SPEC, inference_by_crops=True)
segmentation_inference_using_thresholds = make_bbox_inference_using_thresholds_func(
    SEGMENTATION_INFERENCE_SPEC,
    make_bbox_inference_func(SEGMENTATION_INFERENCE_SPEC),
)


@dataclass
class Inference_SegmentationModel(PipelineStep):
    input__image: PipelineInput | Sequence[PipelineInput]
    input__segmentation_model: PipelineInput | Sequence[PipelineInput]
    output__segmentation_prediction: PipelineOutput
    primary_keys: List[str]
    chunk_size: int = 64
    create_table: bool = False
    labels: Optional[Labels] = None
    image__image_path__name: str = "image__image_path"
    bbox_id__name: Optional[str] = "bbox_id"
    batch_size_default: int = int(os.environ.get("SEGMENTATION_BATCH_SIZE_DEFAULT", 64))
    executor_config: Optional[ExecutorConfig] = None
    filters: Union[LabelDict, Callable[[], LabelDict], None] = None
    segmentation_model_primary_keys: Optional[List[str]] = None
    prediction_threshold: Optional[float] = None

    def build_compute(self, ds: DataStore, catalog: Catalog) -> List[ComputeStep]:
        return build_bbox_inference_compute(
            ds,
            catalog,
            BboxInferenceStepConfig(
                spec=SEGMENTATION_INFERENCE_SPEC,
                mode=InferenceMode.PLAIN,
                input__image=self.input__image,
                input__model=self.input__segmentation_model,
                output__prediction=self.output__segmentation_prediction,
                primary_keys=self.primary_keys,
                chunk_size=self.chunk_size,
                create_table=self.create_table,
                labels=self.labels,
                image__image_path__name=self.image__image_path__name,
                bbox_id__name=self.bbox_id__name,
                batch_size_default=self.batch_size_default,
                executor_config=self.executor_config,
                filters=self.filters,
                model_primary_keys=self.segmentation_model_primary_keys,
                prediction_threshold=self.prediction_threshold,
            ),
        )


@dataclass
class InferenceBySplitOnCrops_SegmentationModel(PipelineStep):
    input__image: PipelineInput | Sequence[PipelineInput]
    input__segmentation_model: PipelineInput | Sequence[PipelineInput]
    output__segmentation_prediction: PipelineOutput
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
    batch_size_default: int = int(os.environ.get("SEGMENTATION_BATCH_SIZE_DEFAULT", 4))
    executor_config: Optional[ExecutorConfig] = None
    filters: Union[LabelDict, Callable[[], LabelDict], None] = None
    segmentation_model_primary_keys: Optional[List[str]] = None
    prediction_threshold: Optional[float] = None

    def build_compute(self, ds: DataStore, catalog: Catalog) -> List[ComputeStep]:
        return build_bbox_inference_compute(
            ds,
            catalog,
            BboxInferenceStepConfig(
                spec=SEGMENTATION_INFERENCE_SPEC,
                mode=InferenceMode.CROPS,
                input__image=self.input__image,
                input__model=self.input__segmentation_model,
                output__prediction=self.output__segmentation_prediction,
                primary_keys=self.primary_keys,
                chunk_size=self.chunk_size,
                create_table=self.create_table,
                labels=self.labels,
                image__image_path__name=self.image__image_path__name,
                bbox_id__name=self.bbox_id__name,
                batch_size_default=self.batch_size_default,
                executor_config=self.executor_config,
                filters=self.filters,
                model_primary_keys=self.segmentation_model_primary_keys,
                prediction_threshold=self.prediction_threshold,
                hCrossing=self.hCrossing,
                vCrossing=self.vCrossing,
                thresholdSpace=self.thresholdSpace,
                blockWidth=self.blockWidth,
                blockHeight=self.blockHeight,
            ),
        )


@dataclass
class Inference_UsingThresholdsPerClasss_SegmentationModel(PipelineStep):
    input__image: PipelineInput | Sequence[PipelineInput]
    input__segmentation_model: PipelineInput | Sequence[PipelineInput]
    input__segmentation_model_thresholds: PipelineInput
    output__segmentation_prediction: PipelineOutput
    primary_keys: List[str]
    chunk_size: int = 64
    create_table: bool = False
    labels: Optional[Labels] = None
    image__image_path__name: str = "image__image_path"
    bbox_id__name: Optional[str] = "bbox_id"
    batch_size_default: int = int(os.environ.get("SEGMENTATION_BATCH_SIZE_DEFAULT", 64))
    executor_config: Optional[ExecutorConfig] = None
    filters: Union[LabelDict, Callable[[], LabelDict], None] = None
    segmentation_model_primary_keys: Optional[List[str]] = None
    class_name_to_threshold__name: str = "class_name_to_threshold"

    def build_compute(self, ds: DataStore, catalog: Catalog) -> List[ComputeStep]:
        return build_bbox_inference_compute(
            ds,
            catalog,
            BboxInferenceStepConfig(
                spec=SEGMENTATION_INFERENCE_SPEC,
                mode=InferenceMode.THRESHOLDS,
                input__image=self.input__image,
                input__model=self.input__segmentation_model,
                input__model_thresholds=self.input__segmentation_model_thresholds,
                output__prediction=self.output__segmentation_prediction,
                primary_keys=self.primary_keys,
                chunk_size=self.chunk_size,
                create_table=self.create_table,
                labels=self.labels,
                image__image_path__name=self.image__image_path__name,
                bbox_id__name=self.bbox_id__name,
                batch_size_default=self.batch_size_default,
                executor_config=self.executor_config,
                filters=self.filters,
                model_primary_keys=self.segmentation_model_primary_keys,
                class_name_to_threshold__name=self.class_name_to_threshold__name,
            ),
        )


Inference_UsingThresholdsPerClass_SegmentationModel = Inference_UsingThresholdsPerClasss_SegmentationModel
