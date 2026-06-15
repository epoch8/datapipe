import os
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import List, Optional, Tuple, Union

import numpy as np
import pandas as pd
from cv_pipeliner import ImageData, PipelineInferencer, YOLOv5_ModelSpec
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
from datapipe.types import Labels, PipelineInput, PipelineOutput, get_pipeline_input_name, get_pipeline_output_name, required_pipeline_input
from sqlalchemy import Column, Float
from sqlalchemy.sql.sqltypes import JSON, Integer, String

from datapipe_ml.core.datapipe import check_columns_are_in_table, normalize_pipeline_inputs, get_datatable
from datapipe_ml.core.image_data import (
    check_if_images_opens,
    convert_df_with_image_data_to_df_with_bbox,
)
from datapipe_ml.inference.bbox_crops import predict_bbox_like_by_crops
from datapipe_ml.metrics.common import stable_unique


def min_prediction_threshold_from_class_thresholds(
    df: pd.DataFrame,
    class_name_to_threshold__name: str,
    *,
    default: float = 0.01,
) -> float:
    mins: list[float] = []
    for mapping in df[class_name_to_threshold__name]:
        if isinstance(mapping, dict) and mapping:
            mins.append(min(float(value) for value in mapping.values()))
    return min(mins) if mins else default


# @cachetools.func.ttl_cache(maxsize=1, ttl=300)
def get_detection_model_spec(
    detection_model__input_size: Tuple[int, int],
    detection_model__model_path: str,
    detection_model__class_names: Tuple[str, ...],
    detection_model__type: str,
) -> DetectionModelSpec:
    if detection_model__type == "yolov5":
        detection_model_spec = YOLOv5_ModelSpec(
            model_path=detection_model__model_path,
            class_names=detection_model__class_names,
            input_size=detection_model__input_size,
        )
    elif detection_model__type == "yolov8":
        detection_model_spec = YOLOv8_ModelSpec(
            model_path=detection_model__model_path,
            class_names=detection_model__class_names,
            input_size=detection_model__input_size,
        )
    else:
        raise ValueError(f"Unknown {detection_model__type=}")
    return detection_model_spec


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
    )


def detection_inference(
    *dfs,
    image__image_path__name: str,
    primary_keys: List[str],
    bbox_id__name: Optional[str],
    batch_size_default: int,
    detection_model_primary_keys: List[str],
    inference_by_crops: bool = False,
    hCrossing: Optional[int] = None,
    vCrossing: Optional[int] = None,
    threseholdSpace: Optional[int] = None,
    blockWidth: Optional[int] = None,
    blockHeight: Optional[int] = None,
    prediction_threshold: Optional[float] = None,
    class_name_to_threshold__name: Optional[str] = None,
):
    df__image: pd.DataFrame = dfs[0]
    if len(dfs) >= 3:
        for df in dfs[1:-1]:
            df__image = pd.merge(df, df__image, on=primary_keys)
    df__detection_model: pd.DataFrame = dfs[-1]
    detection_model_other_primary_keys = [
        primary_key for primary_key in detection_model_primary_keys if primary_key not in primary_keys
    ]
    if bbox_id__name is not None:
        columns = (
            primary_keys
            + detection_model_other_primary_keys
            + [
                bbox_id__name,
                "x_min",
                "y_min",
                "x_max",
                "y_max",
                "label",
                "prediction__detection_score",
            ]
        )
    else:
        columns = (
            primary_keys
            + detection_model_other_primary_keys
            + [
                "bboxes",
                "labels",
                "prediction__detection_scores",
            ]
        )
    if df__image.empty:
        return pd.DataFrame(columns=columns)

    image_paths_to_check = np.array(df__image[image__image_path__name])
    idxs = check_if_images_opens([str(x) for x in image_paths_to_check])
    df__image = df__image.loc[idxs].reset_index(drop=True)

    if len(set(df__image.columns).intersection(set(df__detection_model.columns))) != 0:
        df__cross = pd.merge(df__image, df__detection_model)
    else:
        df__cross = pd.merge(df__image, df__detection_model, how="cross")
    df__predictions_detection = []
    for _, df_grouped in df__cross.groupby(detection_model_primary_keys):
        detection_model_spec = get_detection_model_spec(
            detection_model__input_size=(
                int(df_grouped.iloc[0]["detection_model__input_size"][0]),
                int(df_grouped.iloc[0]["detection_model__input_size"][1]),
            ),
            detection_model__model_path=df_grouped.iloc[0]["detection_model__model_path"],
            detection_model__class_names=tuple(
                [str(class_name) for class_name in df_grouped.iloc[0]["detection_model__class_names"]]
            ),
            detection_model__type=df_grouped.iloc[0]["detection_model__type"],
        )
        detection_model = PipelineModelSpec(
            detection_model_spec=detection_model_spec, classification_model_spec=None
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
                    inferencer=detection_model,
                    detection_score_threshold=(
                        prediction_threshold
                        if prediction_threshold is not None
                        else df_grouped.iloc[0]["detection_model__score_threshold"]
                    ),
                    hCrossing=hCrossing,
                    vCrossing=vCrossing,
                    threseholdSpace=threseholdSpace,
                    blockWidth=blockWidth,
                    blockHeight=blockHeight,
                )
                for image_data in images_data
            ]
        else:
            df_grouped["image_data"] = detection_model.predict(
                images_data,
                detection_score_threshold=(
                    prediction_threshold
                    if prediction_threshold is not None
                    else df_grouped.iloc[0]["detection_model__score_threshold"]
                ),
                disable_tqdm=True,
                batch_size_default=batch_size_default,
            )
        if class_name_to_threshold__name is not None:
            class_name_to_threshold = df_grouped.iloc[0][class_name_to_threshold__name]
            for image_data in df_grouped["image_data"]:
                image_data.bboxes_data = [
                    bbox_data
                    for bbox_data in image_data.bboxes_data
                    if bbox_data.detection_score >= class_name_to_threshold[bbox_data.label]
                ]
        df__prediction_detection = convert_df_with_image_data_to_df_with_bbox(
            df__with_image_data=df_grouped,
            primary_keys=primary_keys + detection_model_other_primary_keys,
            bbox_id__name=bbox_id__name,
            image__image_path__name=image__image_path__name,
        )
        df__predictions_detection.append(df__prediction_detection)

    if len(df__predictions_detection) > 0:
        df__prediction_detection = pd.concat(df__predictions_detection, ignore_index=True)[columns]
    else:
        df__prediction_detection = pd.DataFrame(columns=columns)
    return df__prediction_detection


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
        if self.detection_model_primary_keys is None:
            self.detection_model_primary_keys = ["detection_model_id"]
        check_columns_are_in_table(ds, self.input__image, self.primary_keys)
        input__images = normalize_pipeline_inputs(self.input__image)
        input__image_names = [get_pipeline_input_name(input__image) for input__image in input__images]
        input_detection_model_name = get_pipeline_input_name(self.input__detection_model)
        output_detection_prediction_name = get_pipeline_output_name(self.output__detection_prediction)
        assert any(
            [
                check_columns_are_in_table(ds, input__image, [self.image__image_path__name], raise_exc=False)
                for input__image in input__images
            ]
        )
        dt__input__images = [get_datatable(ds, input__image) for input__image in input__image_names]
        dt__input_detection_model = get_datatable(ds, input_detection_model_name)
        check_columns_are_in_table(
            ds,
            self.input__detection_model,
            self.detection_model_primary_keys
            + [
                "detection_model__input_size",
                "detection_model__score_threshold",
                "detection_model__model_path",
                "detection_model__type",
                "detection_model__class_names",
            ],
        )
        # ---
        if self.bbox_id__name is not None:
            catalog.add_datatable(
                output_detection_prediction_name,
                Table(
                    ds.get_or_create_table(
                        output_detection_prediction_name,
                        TableStoreDB(
                            dbconn=ds.meta_dbconn,
                            name=output_detection_prediction_name,
                            data_sql_schema=[
                                column
                                for column in dt__input__images[0].primary_schema
                                if column.name in self.primary_keys
                            ]
                            + [
                                column
                                for column in dt__input_detection_model.primary_schema
                                if column.name not in self.primary_keys
                            ]
                            + [
                                Column(self.bbox_id__name, String, primary_key=True),
                                Column("x_min", Integer),
                                Column("y_min", Integer),
                                Column("x_max", Integer),
                                Column("y_max", Integer),
                                Column("label", String),
                                Column("prediction__detection_score", Float),
                            ],
                            create_table=self.create_table,
                        ),
                    ).table_store
                ),
            )
        else:
            catalog.add_datatable(
                output_detection_prediction_name,
                Table(
                    ds.get_or_create_table(
                        output_detection_prediction_name,
                        TableStoreDB(
                            dbconn=ds.meta_dbconn,
                            name=output_detection_prediction_name,
                            data_sql_schema=[
                                column
                                for column in dt__input__images[0].primary_schema
                                if column.name in self.primary_keys
                            ]
                            + [
                                column
                                for column in dt__input_detection_model.primary_schema
                                if column.name not in self.primary_keys
                            ]
                            + [
                                Column("bboxes", JSON),
                                Column("labels", JSON),
                                Column("prediction__detection_scores", JSON),
                            ],
                            create_table=self.create_table,
                        ),
                    ).table_store
                ),
            )
        # ---
        pipeline = Pipeline(
            [
                BatchTransform(
                    func=detection_inference,
                    inputs=[*[required_pipeline_input(input__image) for input__image in input__images], self.input__detection_model],
                    outputs=[self.output__detection_prediction],
                    transform_keys=stable_unique(self.primary_keys + self.detection_model_primary_keys),
                    chunk_size=self.chunk_size,
                    labels=self.labels,
                    order_by=self.detection_model_primary_keys,
                    order="desc",
                    kwargs=dict(
                        primary_keys=self.primary_keys,
                        image__image_path__name=self.image__image_path__name,
                        bbox_id__name=self.bbox_id__name,
                        batch_size_default=self.batch_size_default,
                        detection_model_primary_keys=self.detection_model_primary_keys,
                        prediction_threshold=self.prediction_threshold,
                    ),
                    executor_config=self.executor_config,
                    filters=self.filters,
                ),
            ]
        )
        return build_compute(ds, catalog, pipeline)


def detection_inference_by_crops(
    *dfs,
    image__image_path__name: str,
    primary_keys: List[str],
    bbox_id__name: str,
    detection_model_primary_keys: List[str],
    batch_size_default: int,
    hCrossing: int,
    vCrossing: int,
    threseholdSpace: int,
    blockWidth: int,
    blockHeight: int,
    prediction_threshold: Optional[float],
):
    return detection_inference(
        *dfs,
        image__image_path__name=image__image_path__name,
        primary_keys=primary_keys,
        bbox_id__name=bbox_id__name,
        detection_model_primary_keys=detection_model_primary_keys,
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
class InferenceBySplitOnCrops_DetectionModel(PipelineStep):
    input__image: PipelineInput | Sequence[PipelineInput]
    input__detection_model: PipelineInput
    output__detection_prediction: PipelineOutput
    primary_keys: List[str]
    hCrossing: int
    vCrossing: int
    threseholdSpace: int
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
        if self.detection_model_primary_keys is None:
            self.detection_model_primary_keys = ["detection_model_id"]
        check_columns_are_in_table(ds, self.input__image, self.primary_keys)
        input__images = normalize_pipeline_inputs(self.input__image)
        input__image_names = [get_pipeline_input_name(input__image) for input__image in input__images]
        input_detection_model_name = get_pipeline_input_name(self.input__detection_model)
        output_detection_prediction_name = get_pipeline_output_name(self.output__detection_prediction)
        assert any(
            [
                check_columns_are_in_table(ds, input__image, [self.image__image_path__name], raise_exc=False)
                for input__image in input__images
            ]
        )
        dt__input__images = [get_datatable(ds, input__image) for input__image in input__image_names]
        dt__input_detection_model = get_datatable(ds, input_detection_model_name)
        check_columns_are_in_table(
            ds,
            self.input__detection_model,
            self.detection_model_primary_keys
            + [
                "detection_model__input_size",
                "detection_model__score_threshold",
                "detection_model__model_path",
                "detection_model__type",
                "detection_model__class_names",
            ],
        )
        # ---
        if self.bbox_id__name is not None:
            catalog.add_datatable(
                output_detection_prediction_name,
                Table(
                    ds.get_or_create_table(
                        output_detection_prediction_name,
                        TableStoreDB(
                            dbconn=ds.meta_dbconn,
                            name=output_detection_prediction_name,
                            data_sql_schema=[
                                column
                                for column in dt__input__images[0].primary_schema
                                if column.name in self.primary_keys
                            ]
                            + [
                                column
                                for column in dt__input_detection_model.primary_schema
                                if column.name not in self.primary_keys
                            ]
                            + [
                                Column(self.bbox_id__name, String, primary_key=True),
                                Column("x_min", Integer),
                                Column("y_min", Integer),
                                Column("x_max", Integer),
                                Column("y_max", Integer),
                                Column("label", String),
                                Column("prediction__detection_score", Float),
                            ],
                            create_table=self.create_table,
                        ),
                    ).table_store
                ),
            )
        else:
            catalog.add_datatable(
                output_detection_prediction_name,
                Table(
                    ds.get_or_create_table(
                        output_detection_prediction_name,
                        TableStoreDB(
                            dbconn=ds.meta_dbconn,
                            name=output_detection_prediction_name,
                            data_sql_schema=[
                                column
                                for column in dt__input__images[0].primary_schema
                                if column.name in self.primary_keys
                            ]
                            + [
                                column
                                for column in dt__input_detection_model.primary_schema
                                if column.name not in self.primary_keys
                            ]
                            + [
                                Column("bboxes", JSON),
                                Column("labels", JSON),
                                Column("prediction__detection_scores", JSON),
                            ],
                            create_table=self.create_table,
                        ),
                    ).table_store
                ),
            )
        # ---
        pipeline = Pipeline(
            [
                BatchTransform(
                    func=detection_inference_by_crops,
                    inputs=[*[required_pipeline_input(input__image) for input__image in input__images], self.input__detection_model],
                    outputs=[self.output__detection_prediction],
                    transform_keys=stable_unique(self.primary_keys + self.detection_model_primary_keys),
                    chunk_size=self.chunk_size,
                    labels=self.labels,
                    order_by=self.detection_model_primary_keys,
                    order="desc",
                    kwargs=dict(
                        primary_keys=self.primary_keys,
                        image__image_path__name=self.image__image_path__name,
                        bbox_id__name=self.bbox_id__name,
                        batch_size_default=self.batch_size_default,
                        hCrossing=self.hCrossing,
                        vCrossing=self.vCrossing,
                        threseholdSpace=self.threseholdSpace,
                        blockWidth=self.blockWidth,
                        blockHeight=self.blockHeight,
                        detection_model_primary_keys=self.detection_model_primary_keys,
                        prediction_threshold=self.prediction_threshold,
                    ),
                    executor_config=self.executor_config,
                    filters=self.filters,
                ),
            ]
        )
        return build_compute(ds, catalog, pipeline)


def detection_inference_using_thresholds(
    *dfs,
    image__image_path__name: str,
    primary_keys: List[str],
    bbox_id__name: str,
    batch_size_default: int,
    detection_model_primary_keys: List[str],
    class_name_to_threshold__name: str,
):
    df__detection_model = pd.merge(dfs[-2], dfs[-1], on=detection_model_primary_keys)
    dfs_list = list(dfs)[:-2] + [df__detection_model]
    prediction_threshold = min_prediction_threshold_from_class_thresholds(
        df__detection_model,
        class_name_to_threshold__name,
    )
    return detection_inference(
        *dfs_list,
        image__image_path__name=image__image_path__name,
        primary_keys=primary_keys,
        bbox_id__name=bbox_id__name,
        batch_size_default=batch_size_default,
        detection_model_primary_keys=detection_model_primary_keys,
        prediction_threshold=prediction_threshold,
        class_name_to_threshold__name=class_name_to_threshold__name,
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
        if self.detection_model_primary_keys is None:
            self.detection_model_primary_keys = ["detection_model_id"]
        check_columns_are_in_table(ds, self.input__image, self.primary_keys)
        input__images = normalize_pipeline_inputs(self.input__image)
        input__image_names = [get_pipeline_input_name(input__image) for input__image in input__images]
        input_detection_model_name = get_pipeline_input_name(self.input__detection_model)
        output_detection_prediction_name = get_pipeline_output_name(self.output__detection_prediction)
        assert any(
            [
                check_columns_are_in_table(ds, input__image, [self.image__image_path__name], raise_exc=False)
                for input__image in input__images
            ]
        )
        dt__input__images = [get_datatable(ds, input__image) for input__image in input__image_names]
        dt__input_detection_model = get_datatable(ds, input_detection_model_name)
        check_columns_are_in_table(
            ds,
            self.input__detection_model,
            self.detection_model_primary_keys
            + [
                "detection_model__input_size",
                "detection_model__score_threshold",
                "detection_model__model_path",
                "detection_model__type",
                "detection_model__class_names",
            ],
        )
        check_columns_are_in_table(
            ds,
            self.input__detection_model_thresholds,
            self.detection_model_primary_keys + [self.class_name_to_threshold__name],
        )
        # ---
        if self.bbox_id__name is not None:
            catalog.add_datatable(
                output_detection_prediction_name,
                Table(
                    ds.get_or_create_table(
                        output_detection_prediction_name,
                        TableStoreDB(
                            dbconn=ds.meta_dbconn,
                            name=output_detection_prediction_name,
                            data_sql_schema=[
                                column
                                for column in dt__input__images[0].primary_schema
                                if column.name in self.primary_keys
                            ]
                            + [
                                column
                                for column in dt__input_detection_model.primary_schema
                                if column.name not in self.primary_keys
                            ]
                            + [
                                Column(self.bbox_id__name, String, primary_key=True),
                                Column("x_min", Integer),
                                Column("y_min", Integer),
                                Column("x_max", Integer),
                                Column("y_max", Integer),
                                Column("label", String),
                                Column("prediction__detection_score", Float),
                            ],
                            create_table=self.create_table,
                        ),
                    ).table_store
                ),
            )
        else:
            catalog.add_datatable(
                output_detection_prediction_name,
                Table(
                    ds.get_or_create_table(
                        output_detection_prediction_name,
                        TableStoreDB(
                            dbconn=ds.meta_dbconn,
                            name=output_detection_prediction_name,
                            data_sql_schema=[
                                column
                                for column in dt__input__images[0].primary_schema
                                if column.name in self.primary_keys
                            ]
                            + [
                                column
                                for column in dt__input_detection_model.primary_schema
                                if column.name not in self.primary_keys
                            ]
                            + [
                                Column("bboxes", JSON),
                                Column("labels", JSON),
                                Column("prediction__detection_scores", JSON),
                            ],
                            create_table=self.create_table,
                        ),
                    ).table_store
                ),
            )
        # ---
        pipeline = Pipeline(
            [
                BatchTransform(
                    func=detection_inference_using_thresholds,
                    inputs=[
                        *[required_pipeline_input(input__image) for input__image in input__images],
                        self.input__detection_model,
                        self.input__detection_model_thresholds,
                    ],
                    outputs=[self.output__detection_prediction],
                    transform_keys=stable_unique(self.primary_keys + self.detection_model_primary_keys),
                    chunk_size=self.chunk_size,
                    labels=self.labels,
                    order_by=self.detection_model_primary_keys,
                    order="desc",
                    kwargs=dict(
                        primary_keys=self.primary_keys,
                        image__image_path__name=self.image__image_path__name,
                        bbox_id__name=self.bbox_id__name,
                        batch_size_default=self.batch_size_default,
                        detection_model_primary_keys=self.detection_model_primary_keys,
                        class_name_to_threshold__name=self.class_name_to_threshold__name,
                    ),
                    executor_config=self.executor_config,
                    filters=self.filters,
                ),
            ]
        )
        return build_compute(ds, catalog, pipeline)
