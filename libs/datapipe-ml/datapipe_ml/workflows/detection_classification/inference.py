import os
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import List, Optional, Tuple, Union

import numpy as np
import pandas as pd
from cv_pipeliner import ImageData
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
from datapipe.types import PipelineInput, PipelineOutput, Labels, required_pipeline_input
from sqlalchemy import Column, Float
from sqlalchemy.sql.sqltypes import Integer, String

from datapipe_ml.core.datapipe import (
    check_columns_are_in_table,
    get_datatable,
    get_pipeline_table_name,
    make_mungled_batch_transform_step_name,
    normalize_pipeline_inputs,
)
from datapipe_ml.inference.model_inputs import (
    build_required_pipeline_inputs,
    primary_model_input,
    wrap_inference_inputs,
)
from datapipe_ml.core.image_data import (
    check_if_images_opens,
    convert_df_with_image_data_to_df_with_bbox,
)
from datapipe_ml.tasks.classification.inference import get_classification_model_spec
from datapipe_ml.tasks.detection.inference import get_detection_model_spec


def get_pipeline_model_spec(
    detection_model__input_size: Tuple[int, int],
    detection_model__model_path: str,
    detection_model__class_names: Tuple[str, ...],
    detection_model__type: str,
    classification_model__input_size: Tuple[int, int],
    classification_model__model_path: str,
    classification_model__class_names: Tuple[str, ...],
    classification_model__type: str,
    classification_model__preprocess_input_script_path: str,
) -> PipelineModelSpec:
    detection_model_spec = get_detection_model_spec(
        detection_model__input_size=detection_model__input_size,
        detection_model__model_path=detection_model__model_path,
        detection_model__class_names=detection_model__class_names,
        detection_model__type=detection_model__type,
    )
    classification_model_spec = get_classification_model_spec(
        classification_model__input_size=classification_model__input_size,
        classification_model__model_path=classification_model__model_path,
        classification_model__class_names=classification_model__class_names,
        classification_model__type=classification_model__type,
        classification_model__preprocess_input_script_path=classification_model__preprocess_input_script_path,
    )
    pipeline_model_spec = PipelineModelSpec(
        detection_model_spec=detection_model_spec, classification_model_spec=classification_model_spec
    )
    return pipeline_model_spec


def pipeline_inference(
    df__image: pd.DataFrame,
    df__detection_model: pd.DataFrame,
    df__classification_model: pd.DataFrame,
    df__pipeline_model: pd.DataFrame,
    image__image_path__name: str,
    primary_keys: List[str],
    bbox_id__name: Optional[str],
    batch_size: int,
    classification_batch_size: int,
    detection_model_primary_keys: List[str],
    classification_model_primary_keys: List[str],
):
    if bbox_id__name is not None:
        columns = (
            primary_keys
            + detection_model_primary_keys
            + classification_model_primary_keys
            + [
                bbox_id__name,
                "x_min",
                "y_min",
                "x_max",
                "y_max",
                "label",
                "prediction__detection_score",
                "prediction__classification_score",
            ]
        )
    else:
        columns = (
            primary_keys
            + detection_model_primary_keys
            + classification_model_primary_keys
            + [
                "bboxes",
                "labels",
                "prediction__detection_scores",
                "prediction__classification_scores",
            ]
        )
    if df__image.empty:
        return pd.DataFrame(columns=columns)
    image_paths_to_check = np.array(df__image[image__image_path__name])
    idxs = check_if_images_opens([str(path) for path in image_paths_to_check])
    df__image = df__image.loc[idxs].reset_index(drop=True)

    df__pipeline_model = pd.merge(df__pipeline_model, df__detection_model, on=["detection_model_id"])
    df__pipeline_model = pd.merge(df__pipeline_model, df__classification_model, on=["classification_model_id"])
    if len(set(df__image.columns).intersection(set(df__pipeline_model.columns))) != 0:
        df__cross = pd.merge(df__image, df__pipeline_model)
    else:
        df__cross = pd.merge(df__image, df__pipeline_model, how="cross")

    df__predictions_pipeline = []
    for _, df_grouped in df__cross.groupby(detection_model_primary_keys + classification_model_primary_keys):
        pipeline_model_spec = get_pipeline_model_spec(
            detection_model__input_size=(
                df_grouped.iloc[0]["detection_model__input_size"][0],
                df_grouped.iloc[0]["detection_model__input_size"][1],
            ),
            detection_model__model_path=df_grouped.iloc[0]["detection_model__model_path"],
            detection_model__class_names=tuple(
                [str(class_name) for class_name in df_grouped.iloc[0]["detection_model__class_names"]]
            ),
            detection_model__type=df_grouped.iloc[0]["detection_model__type"],
            classification_model__input_size=(
                df_grouped.iloc[0]["classification_model__input_size"][0],
                df_grouped.iloc[0]["classification_model__input_size"][1],
            ),
            classification_model__model_path=df_grouped.iloc[0]["classification_model__model_path"],
            classification_model__class_names=tuple(
                [str(class_name) for class_name in df_grouped.iloc[0]["classification_model__class_names"]]
            ),
            classification_model__type=df_grouped.iloc[0]["classification_model__type"],
            classification_model__preprocess_input_script_path=df_grouped.iloc[0][
                "classification_model__preprocess_input_script_path"
            ],
        )
        pipeline_model = pipeline_model_spec.load_pipeline_inferencer()
        images_data = [ImageData(image_path=image_path) for image_path in df_grouped[image__image_path__name]]
        df_grouped["image_data"] = pipeline_model.predict(
            images_data,
            detection_score_threshold=df_grouped.iloc[0]["detection_model__score_threshold"],
            disable_tqdm=True,
            disable_tqdm_classification=True,
            batch_size_default=batch_size,
            classification_batch_size=classification_batch_size,
        )
        df__prediction_detection = convert_df_with_image_data_to_df_with_bbox(
            df__with_image_data=df_grouped,
            primary_keys=primary_keys + detection_model_primary_keys + classification_model_primary_keys,
            bbox_id__name=bbox_id__name,
            image__image_path__name=image__image_path__name,
        )
        df__predictions_pipeline.append(df__prediction_detection)

    if len(df__predictions_pipeline) > 0:
        df__prediction_pipeline = pd.concat(df__predictions_pipeline, ignore_index=True)[columns]
    else:
        df__prediction_pipeline = pd.DataFrame(columns=columns)
    return df__prediction_pipeline


@dataclass
class Inference_PipelineModel(PipelineStep):
    input__image: PipelineInput | Sequence[PipelineInput]
    input__detection_model: PipelineInput | Sequence[PipelineInput]
    input__classification_model: PipelineInput | Sequence[PipelineInput]
    input__pipeline_model: PipelineInput
    output__pipeline_prediction: PipelineOutput
    primary_keys: List[str]
    chunk_size: int = 16
    create_table: bool = False
    labels: Optional[Labels] = None
    image__image_path__name: str = "image__image_path"
    bbox_id__name: Optional[str] = "bbox_id"
    batch_size: int = int(os.environ.get("DETECTION_BATCH_SIZE_DEFAULT", 16))
    classification_batch_size: int = int(os.environ.get("CLASSIFICATION_BATCH_SIZE_DEFAULT", 64))
    executor_config: Optional[ExecutorConfig] = None
    filters: Union[LabelDict, Callable[[], LabelDict], None] = None

    def build_compute(self, ds: DataStore, catalog: Catalog) -> List[ComputeStep]:
        primary_detection_model = primary_model_input(self.input__detection_model)
        primary_classification_model = primary_model_input(self.input__classification_model)
        dt__input__detection_model = get_datatable(ds, primary_detection_model)
        dt__input__classification_model = get_datatable(ds, primary_classification_model)
        assert (
            len(set(dt__input__detection_model.primary_keys).intersection(dt__input__classification_model.primary_keys))
            == 0
        )
        check_columns_are_in_table(ds, self.input__image, self.primary_keys)
        input__images = normalize_pipeline_inputs(self.input__image)
        image_pipeline_inputs = build_required_pipeline_inputs(self.input__image)
        detection_model_pipeline_inputs = build_required_pipeline_inputs(self.input__detection_model)
        classification_model_pipeline_inputs = build_required_pipeline_inputs(self.input__classification_model)
        assert any(
            [
                check_columns_are_in_table(ds, input__image, [self.image__image_path__name], raise_exc=False)
                for input__image in input__images
            ]
        )
        dt__input__images = [get_datatable(ds, input__image) for input__image in input__images]
        check_columns_are_in_table(
            ds,
            primary_detection_model,
            [
                "detection_model__input_size",
                "detection_model__score_threshold",
                "detection_model__model_path",
                "detection_model__type",
                "detection_model__class_names",
            ],
        )
        for model_filter in normalize_pipeline_inputs(self.input__detection_model)[1:]:
            check_columns_are_in_table(ds, model_filter, dt__input__detection_model.primary_keys)
        check_columns_are_in_table(
            ds,
            primary_classification_model,
            [
                "classification_model__input_size",
                "classification_model__model_path",
                "classification_model__type",
                "classification_model__class_names",
            ],
        )
        for model_filter in normalize_pipeline_inputs(self.input__classification_model)[1:]:
            check_columns_are_in_table(ds, model_filter, dt__input__classification_model.primary_keys)
        check_columns_are_in_table(
            ds,
            self.input__pipeline_model,
            dt__input__detection_model.primary_keys + dt__input__classification_model.primary_keys,
        )
        # ---
        catalog.add_datatable(
            get_pipeline_table_name(self.output__pipeline_prediction),
            Table(
                ds.get_or_create_table(
                    get_pipeline_table_name(self.output__pipeline_prediction),
                    TableStoreDB(
                        dbconn=ds.meta_dbconn,
                        name=get_pipeline_table_name(self.output__pipeline_prediction),
                        data_sql_schema=[
                            column for column in dt__input__images[0].primary_schema if column.name in self.primary_keys
                        ]
                        + dt__input__detection_model.primary_schema
                        + dt__input__classification_model.primary_schema
                        + [
                            Column(self.bbox_id__name, String, primary_key=True),
                            Column("x_min", Integer),
                            Column("y_min", Integer),
                            Column("x_max", Integer),
                            Column("y_max", Integer),
                            Column("label", String),
                            Column("prediction__detection_score", Float),
                            Column("prediction__classification_score", Float),
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
                    func=wrap_inference_inputs(
                        pipeline_inference,
                        n_image_inputs=len(image_pipeline_inputs),
                        primary_keys=self.primary_keys,
                        model_input_groups=[
                            (len(detection_model_pipeline_inputs), dt__input__detection_model.primary_keys),
                            (len(classification_model_pipeline_inputs), dt__input__classification_model.primary_keys),
                        ],
                        n_trailing_inputs=1,
                    ),
                    name=make_mungled_batch_transform_step_name(
                        ds,
                        catalog,
                        base_name="pipeline_inference",
                        inputs=[
                            *image_pipeline_inputs,
                            *detection_model_pipeline_inputs,
                            *classification_model_pipeline_inputs,
                            required_pipeline_input(self.input__pipeline_model),
                        ],
                        outputs=[self.output__pipeline_prediction],
                    ),
                    inputs=[
                        *image_pipeline_inputs,
                        *detection_model_pipeline_inputs,
                        *classification_model_pipeline_inputs,
                        required_pipeline_input(self.input__pipeline_model),
                    ],
                    outputs=[self.output__pipeline_prediction],
                    transform_keys=self.primary_keys
                    + dt__input__detection_model.primary_keys
                    + dt__input__classification_model.primary_keys,
                    chunk_size=self.chunk_size,
                    labels=self.labels,
                    order_by=dt__input__detection_model.primary_keys + dt__input__classification_model.primary_keys,
                    order="desc",
                    executor_config=self.executor_config,
                    kwargs=dict(
                        primary_keys=self.primary_keys,
                        image__image_path__name=self.image__image_path__name,
                        bbox_id__name=self.bbox_id__name,
                        batch_size=self.batch_size,
                        classification_batch_size=self.classification_batch_size,
                        detection_model_primary_keys=dt__input__detection_model.primary_keys,
                        classification_model_primary_keys=dt__input__classification_model.primary_keys,
                    ),
                    filters=self.filters,
                ),
            ]
        )
        return build_compute(ds, catalog, pipeline)
