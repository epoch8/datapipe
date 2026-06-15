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
from sqlalchemy import Column, Float
from sqlalchemy.sql.sqltypes import JSON, Integer, String

from datapipe_ml.inference.bbox_pipeline import (
    BboxInferenceStepConfig,
    BboxTaskInferenceSpec,
    InferenceMode,
    build_bbox_inference_compute,
    make_bbox_inference_func,
    run_bbox_task_inference,
)

_MODEL_COLUMNS = [
    "keypoints_model__input_size",
    "keypoints_model__score_threshold",
    "keypoints_model__model_path",
    "keypoints_model__type",
    "keypoints_model__class_names",
]


def get_keypoints_model_spec(
    keypoints_model__input_size: Tuple[int, int],
    keypoints_model__model_path: str,
    keypoints_model__class_names: Tuple[str, ...],
    keypoints_model__type: str,
) -> DetectionModelSpec:
    if keypoints_model__type in ["yolov8", "yolov8_pose"]:
        return YOLOv8_ModelSpec(
            model_path=keypoints_model__model_path,
            class_names=keypoints_model__class_names,
            input_size=keypoints_model__input_size,
        )
    raise ValueError(f"Unknown {keypoints_model__type=}")


def _get_keypoints_model_spec(
    *,
    input_size: Tuple[int, int],
    model_path: str,
    class_names: Tuple[str, ...],
    model_type: str,
) -> DetectionModelSpec:
    return get_keypoints_model_spec(
        keypoints_model__input_size=input_size,
        keypoints_model__model_path=model_path,
        keypoints_model__class_names=class_names,
        keypoints_model__type=model_type,
    )


KEYPOINTS_INFERENCE_SPEC = BboxTaskInferenceSpec(
    model_prefix="keypoints_model",
    default_model_primary_keys=["keypoints_model_id"],
    model_columns=_MODEL_COLUMNS,
    get_model_spec=_get_keypoints_model_spec,
    model_primary_keys_kwarg="keypoints_model_primary_keys",
    crop_include_keypoints=True,
    crop_model_input_size_from_row=True,
    extra_bbox_id_after_coords=(Column("keypoints", JSON),),
    extra_bbox_id_after_score=(Column("prediction__keypoints_scores", JSON),),
    extra_json_after_bboxes=(Column("keypoints", JSON),),
    extra_json_after_scores=(Column("prediction__keypoints_scores", JSON),),
)


def keypoints_inference(*dfs, **kwargs):
    if "keypoints_model_primary_keys" in kwargs:
        kwargs["model_primary_keys"] = kwargs.pop("keypoints_model_primary_keys")
    return run_bbox_task_inference(KEYPOINTS_INFERENCE_SPEC, *dfs, **kwargs)


keypoints_inference_by_crops = make_bbox_inference_func(KEYPOINTS_INFERENCE_SPEC, inference_by_crops=True)


@dataclass
class Inference_KeypointsModel(PipelineStep):
    input__image: PipelineInput | Sequence[PipelineInput]
    input__keypoints_model: PipelineInput
    output__keypoints_prediction: PipelineOutput
    primary_keys: List[str]
    chunk_size: int = 64
    create_table: bool = False
    labels: Optional[Labels] = None
    image__image_path__name: str = "image__image_path"
    bbox_id__name: Optional[str] = "bbox_id"
    batch_size_default: int = int(os.environ.get("KEYPOINTS_BATCH_SIZE_DEFAULT", 8))
    executor_config: Optional[ExecutorConfig] = None
    filters: Union[LabelDict, Callable[[], LabelDict], None] = None
    keypoints_model_primary_keys: Optional[List[str]] = None
    prediction_threshold: Optional[float] = None

    def build_compute(self, ds: DataStore, catalog: Catalog) -> List[ComputeStep]:
        return build_bbox_inference_compute(
            ds,
            catalog,
            BboxInferenceStepConfig(
                spec=KEYPOINTS_INFERENCE_SPEC,
                mode=InferenceMode.PLAIN,
                input__image=self.input__image,
                input__model=self.input__keypoints_model,
                output__prediction=self.output__keypoints_prediction,
                primary_keys=self.primary_keys,
                chunk_size=self.chunk_size,
                create_table=self.create_table,
                labels=self.labels,
                image__image_path__name=self.image__image_path__name,
                bbox_id__name=self.bbox_id__name,
                batch_size_default=self.batch_size_default,
                executor_config=self.executor_config,
                filters=self.filters,
                model_primary_keys=self.keypoints_model_primary_keys,
                prediction_threshold=self.prediction_threshold,
            ),
        )


@dataclass
class InferenceBySplitOnCrops_KeypointsModel(PipelineStep):
    input__image: PipelineInput | Sequence[PipelineInput]
    input__keypoints_model: PipelineInput
    output__keypoints_prediction: PipelineOutput
    primary_keys: List[str]
    hCrossing: int = 400
    vCrossing: int = 400
    thresholdSpace: int = 160
    blockWidth: int = 1024
    blockHeight: int = 1024
    chunk_size: int = 64
    create_table: bool = False
    labels: Optional[Labels] = None
    image__image_path__name: str = "image__image_path"
    bbox_id__name: Optional[str] = "bbox_id"
    batch_size_default: int = int(os.environ.get("KEYPOINTS_BATCH_SIZE_DEFAULT", 4))
    executor_config: Optional[ExecutorConfig] = None
    filters: Union[LabelDict, Callable[[], LabelDict], None] = None
    keypoints_model_primary_keys: Optional[List[str]] = None
    prediction_threshold: Optional[float] = None

    def build_compute(self, ds: DataStore, catalog: Catalog) -> List[ComputeStep]:
        return build_bbox_inference_compute(
            ds,
            catalog,
            BboxInferenceStepConfig(
                spec=KEYPOINTS_INFERENCE_SPEC,
                mode=InferenceMode.CROPS,
                input__image=self.input__image,
                input__model=self.input__keypoints_model,
                output__prediction=self.output__keypoints_prediction,
                primary_keys=self.primary_keys,
                chunk_size=self.chunk_size,
                create_table=self.create_table,
                labels=self.labels,
                image__image_path__name=self.image__image_path__name,
                bbox_id__name=self.bbox_id__name,
                batch_size_default=self.batch_size_default,
                executor_config=self.executor_config,
                filters=self.filters,
                model_primary_keys=self.keypoints_model_primary_keys,
                prediction_threshold=self.prediction_threshold,
                hCrossing=self.hCrossing,
                vCrossing=self.vCrossing,
                thresholdSpace=self.thresholdSpace,
                blockWidth=self.blockWidth,
                blockHeight=self.blockHeight,
            ),
        )
