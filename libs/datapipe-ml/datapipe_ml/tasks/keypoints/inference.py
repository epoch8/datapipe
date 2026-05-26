import os
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import List, Optional, Tuple, Union, cast

import numpy as np
import pandas as pd
from cv_pipeliner import ImageData, PipelineInferencer
from cv_pipeliner.inferencers.detection.core import DetectionModelSpec
from cv_pipeliner.inferencers.detection.yolov8 import YOLOv8_ModelSpec
from cv_pipeliner.inferencers.pipeline import PipelineModelSpec
from datapipe.compute import (
    Catalog,
    ComputeStep,
    Pipeline,
    PipelineStep,
    Table,
    build_compute,
)
from datapipe.datatable import DataStore
from datapipe.executor import ExecutorConfig
from datapipe.run_config import LabelDict
from datapipe.step.batch_transform import BatchTransform
from datapipe.store.database import TableStoreDB
from datapipe.types import required_pipeline_input, PipelineInput, PipelineOutput, Labels
from sqlalchemy import Column, Float
from sqlalchemy.sql.sqltypes import JSON, Integer, String

from datapipe_ml.core.datapipe import check_columns_are_in_table, normalize_pipeline_inputs
from datapipe_ml.core.image_data import (
    check_if_images_opens,
    convert_df_with_image_data_to_df_with_bbox,
)
from datapipe_ml.inference.bbox_crops import predict_bbox_like_by_crops
from datapipe_ml.metrics.common import stable_unique


def get_keypoints_model_spec(
    keypoints_model__input_size: Tuple[int, int],
    keypoints_model__model_path: str,
    keypoints_model__class_names: Tuple[str, ...],
    keypoints_model__type: str,
) -> DetectionModelSpec:
    if keypoints_model__type in ["yolov8", "yolov8_pose"]:
        keypoints_model_spec = YOLOv8_ModelSpec(
            model_path=keypoints_model__model_path,
            class_names=keypoints_model__class_names,
            input_size=keypoints_model__input_size,
        )
    else:
        raise ValueError(f"Unknown {keypoints_model__type=}")
    return keypoints_model_spec


def predict_by_crops(
    image_data: ImageData,
    inferencer: PipelineInferencer,
    detection_score_threshold: float,
    hCrossing: int,
    vCrossing: int,
    threseholdSpace: int,
    blockWidth: int,
    blockHeight: int,
    model_input_size: Tuple[int, int] = (640, 640),
) -> ImageData:
    return predict_bbox_like_by_crops(
        image_data=image_data,
        inferencer=inferencer,
        detection_score_threshold=detection_score_threshold,
        h_crossing=hCrossing,
        v_crossing=vCrossing,
        threshold_space=threseholdSpace,
        block_width=blockWidth,
        block_height=blockHeight,
        model_input_size=model_input_size,
        include_keypoints=True,
    )


def keypoints_inference(
    *dfs,
    image__image_path__name: str,
    primary_keys: List[str],
    bbox_id__name: Optional[str],
    batch_size_default: int,
    keypoints_model_primary_keys: List[str],
    inference_by_crops: bool = False,
    hCrossing: Optional[int] = None,
    vCrossing: Optional[int] = None,
    threseholdSpace: Optional[int] = None,
    blockWidth: Optional[int] = None,
    blockHeight: Optional[int] = None,
    prediction_threshold: Optional[float] = None,
):
    df__image: pd.DataFrame = dfs[0]
    if len(dfs) >= 3:
        for df in dfs[1:-1]:
            df__image = pd.merge(df, df__image, on=primary_keys)
    df__keypoints_model: pd.DataFrame = dfs[-1]
    keypoints_model_other_primary_keys = [
        primary_key for primary_key in keypoints_model_primary_keys if primary_key not in primary_keys
    ]
    if bbox_id__name is not None:
        columns = (
            primary_keys
            + keypoints_model_other_primary_keys
            + [
                bbox_id__name,
                "x_min",
                "y_min",
                "x_max",
                "y_max",
                "keypoints",
                "label",
                "prediction__detection_score",
                "prediction__keypoint_scores",
            ]
        )
    else:
        columns = (
            primary_keys
            + keypoints_model_other_primary_keys
            + ["bboxes", "keypoints", "labels", "prediction__detection_scores", "prediction__keypoints_scores"]
        )
    if df__image.empty:
        return pd.DataFrame(columns=columns)

    image_paths_to_check = np.array(df__image[image__image_path__name])
    idxs = check_if_images_opens([str(x) for x in image_paths_to_check])
    df__image = df__image.loc[idxs].reset_index(drop=True)

    if len(set(df__image.columns).intersection(set(df__keypoints_model.columns))) != 0:
        df__cross = pd.merge(df__image, df__keypoints_model)
    else:
        df__cross = pd.merge(df__image, df__keypoints_model, how="cross")
    df__predictions_keypoints = []
    for _, df_grouped in df__cross.groupby(keypoints_model_primary_keys):
        score_threshold = (
            prediction_threshold
            if prediction_threshold is not None
            else float(df_grouped.iloc[0]["keypoints_model__score_threshold"])
        )
        model_input_size = (
            int(df_grouped.iloc[0]["keypoints_model__input_size"][0]),
            int(df_grouped.iloc[0]["keypoints_model__input_size"][1]),
        )
        keypoints_model_spec = get_keypoints_model_spec(
            keypoints_model__input_size=model_input_size,
            keypoints_model__model_path=df_grouped.iloc[0]["keypoints_model__model_path"],
            keypoints_model__class_names=tuple(
                [str(class_name) for class_name in df_grouped.iloc[0]["keypoints_model__class_names"]]
            ),
            keypoints_model__type=df_grouped.iloc[0]["keypoints_model__type"],
        )
        keypoints_model = PipelineModelSpec(
            detection_model_spec=keypoints_model_spec,
            classification_model_spec=None,
        ).load_pipeline_inferencer()
        images_data = [ImageData(image_path=image_path) for image_path in df_grouped[image__image_path__name]]
        if inference_by_crops:
            assert hCrossing is not None
            assert vCrossing is not None
            assert threseholdSpace is not None
            assert blockWidth is not None
            assert blockHeight is not None
            df_grouped["image_data"] = [
                predict_by_crops(
                    image_data=image_data,
                    inferencer=keypoints_model,
                    detection_score_threshold=score_threshold,
                    hCrossing=hCrossing,
                    vCrossing=vCrossing,
                    threseholdSpace=threseholdSpace,
                    blockWidth=blockWidth,
                    blockHeight=blockHeight,
                    model_input_size=model_input_size,
                )
                for image_data in images_data
            ]
        else:
            df_grouped["image_data"] = keypoints_model.predict(
                images_data,
                detection_score_threshold=score_threshold,
                disable_tqdm=True,
                batch_size_default=batch_size_default,
            )
        df__prediction_keypoints = convert_df_with_image_data_to_df_with_bbox(
            df__with_image_data=df_grouped,
            primary_keys=primary_keys + keypoints_model_other_primary_keys,
            bbox_id__name=bbox_id__name,
            image__image_path__name=image__image_path__name,
        )
        df__predictions_keypoints.append(df__prediction_keypoints)

    if len(df__predictions_keypoints) > 0:
        df__prediction_keypoints = pd.concat(df__predictions_keypoints, ignore_index=True)[columns]
    else:
        df__prediction_keypoints = pd.DataFrame(columns=columns)
    return df__prediction_keypoints


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
        if self.keypoints_model_primary_keys is None:
            self.keypoints_model_primary_keys = ["keypoints_model_id"]
        check_columns_are_in_table(ds, self.input__image, self.primary_keys)
        input__images = normalize_pipeline_inputs(self.input__image)
        assert any(
            [
                check_columns_are_in_table(ds, input__image, [self.image__image_path__name], raise_exc=False)
                for input__image in input__images
            ]
        )
        dt__input__images = [ds.get_table(input__image) for input__image in input__images]
        dt__input_keypoints_model = ds.get_table(self.input__keypoints_model)
        check_columns_are_in_table(
            ds,
            self.input__keypoints_model,
            self.keypoints_model_primary_keys
            + [
                "keypoints_model__input_size",
                "keypoints_model__score_threshold",
                "keypoints_model__model_path",
                "keypoints_model__type",
                "keypoints_model__class_names",
            ],
        )
        if self.bbox_id__name is not None:
            extra_columns: List[Column] = [
                Column(self.bbox_id__name, String, primary_key=True),
                Column("x_min", Integer),
                Column("y_min", Integer),
                Column("x_max", Integer),
                Column("y_max", Integer),
                Column("keypoints", JSON),
                Column("label", String),
                Column("prediction__detection_score", Float),
                Column("prediction__keypoint_scores", JSON),
            ]
        else:
            extra_columns = [
                Column("bboxes", JSON),
                Column("keypoints", JSON),
                Column("labels", JSON),
                Column("prediction__detection_scores", JSON),
                Column("prediction__keypoints_scores", JSON),
            ]
        catalog.add_datatable(
            self.output__keypoints_prediction,
            Table(
                ds.get_or_create_table(
                    self.output__keypoints_prediction,
                    TableStoreDB(
                        dbconn=ds.meta_dbconn,
                        name=self.output__keypoints_prediction,
                        data_sql_schema=[
                            column for column in dt__input__images[0].primary_schema if column.name in self.primary_keys
                        ]
                        + cast(
                            List[Column],
                            [
                                column
                                for column in dt__input_keypoints_model.primary_schema
                                if column.name not in self.primary_keys
                            ],
                        )
                        + extra_columns,
                        create_table=self.create_table,
                    ),
                ).table_store
            ),
        )
        pipeline = Pipeline(
            [
                BatchTransform(
                    func=keypoints_inference,
                    inputs=[*[required_pipeline_input(input__image) for input__image in input__images], self.input__keypoints_model],
                    outputs=[self.output__keypoints_prediction],
                    transform_keys=stable_unique(self.primary_keys + self.keypoints_model_primary_keys),
                    chunk_size=self.chunk_size,
                    labels=self.labels,
                    order_by=self.keypoints_model_primary_keys,
                    order="desc",
                    kwargs=dict(
                        primary_keys=self.primary_keys,
                        image__image_path__name=self.image__image_path__name,
                        bbox_id__name=self.bbox_id__name,
                        batch_size_default=self.batch_size_default,
                        keypoints_model_primary_keys=self.keypoints_model_primary_keys,
                        prediction_threshold=self.prediction_threshold,
                    ),
                    executor_config=self.executor_config,
                    filters=self.filters,
                ),
            ]
        )
        return build_compute(ds, catalog, pipeline)


def keypoints_inference_by_crops(
    *dfs,
    image__image_path__name: str,
    primary_keys: List[str],
    bbox_id__name: str,
    keypoints_model_primary_keys: List[str],
    batch_size_default: int,
    hCrossing: int,
    vCrossing: int,
    threseholdSpace: int,
    blockWidth: int,
    blockHeight: int,
    prediction_threshold: Optional[float],
):
    return keypoints_inference(
        *dfs,
        image__image_path__name=image__image_path__name,
        primary_keys=primary_keys,
        bbox_id__name=bbox_id__name,
        keypoints_model_primary_keys=keypoints_model_primary_keys,
        batch_size_default=batch_size_default,
        inference_by_crops=True,
        hCrossing=hCrossing,
        vCrossing=vCrossing,
        threseholdSpace=threseholdSpace,
        blockWidth=blockWidth,
        blockHeight=blockHeight,
        prediction_threshold=prediction_threshold,
    )


@dataclass
class InferenceBySplitOnCrops_KeypointsModel(Inference_KeypointsModel):
    hCrossing: int = 400
    vCrossing: int = 400
    threseholdSpace: int = 160
    blockWidth: int = 1024
    blockHeight: int = 1024
    batch_size_default: int = int(os.environ.get("KEYPOINTS_BATCH_SIZE_DEFAULT", 4))

    def build_compute(self, ds: DataStore, catalog: Catalog) -> List[ComputeStep]:
        if self.keypoints_model_primary_keys is None:
            self.keypoints_model_primary_keys = ["keypoints_model_id"]
        steps = super().build_compute(ds, catalog)
        for step in steps:
            if isinstance(step, BatchTransform):
                step.func = keypoints_inference_by_crops
                step.kwargs.update(
                    dict(
                        hCrossing=self.hCrossing,
                        vCrossing=self.vCrossing,
                        threseholdSpace=self.threseholdSpace,
                        blockWidth=self.blockWidth,
                        blockHeight=self.blockHeight,
                    )
                )
        return steps
