from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from enum import Enum
from typing import List, Optional, Tuple, Union

import numpy as np
import pandas as pd
from cv_pipeliner import ImageData
from cv_pipeliner.inferencers.detection.core import DetectionModelSpec
from cv_pipeliner.inferencers.pipeline import PipelineModelSpec
from datapipe.compute import Catalog, ComputeStep, Pipeline, Table, build_compute
from datapipe.datatable import DataStore
from datapipe.executor import ExecutorConfig
from datapipe.run_config import LabelDict
from datapipe.step.batch_transform import BatchTransform
from datapipe.store.database import TableStoreDB
from datapipe.types import Labels, PipelineInput, PipelineOutput, get_pipeline_input_name, get_pipeline_output_name, required_pipeline_input
from sqlalchemy import Column, Float
from sqlalchemy.sql.sqltypes import JSON, Integer, String

from datapipe_ml.core.datapipe import check_columns_are_in_table, get_datatable, normalize_pipeline_inputs
from datapipe_ml.core.image_data import check_if_images_opens, convert_df_with_image_data_to_df_with_bbox
from datapipe_ml.inference.bbox_crops import predict_by_crops
from datapipe_ml.inference.common import min_prediction_threshold_from_class_thresholds, resolve_threshold_space
from datapipe_ml.metrics.common import stable_unique

class InferenceMode(str, Enum):
    PLAIN = "plain"
    CROPS = "crops"
    THRESHOLDS = "thresholds"


@dataclass(frozen=True)
class BboxTaskInferenceSpec:
    model_prefix: str
    default_model_primary_keys: List[str]
    model_columns: List[str]
    get_model_spec: Callable[..., DetectionModelSpec]
    model_primary_keys_kwarg: str = "model_primary_keys"
    crop_include_masks: bool = False
    crop_include_keypoints: bool = False
    crop_model_input_size: Tuple[int, int] = (640, 640)
    crop_model_input_size_from_row: bool = False
    extra_bbox_id_after_coords: Tuple[Column, ...] = ()
    extra_bbox_id_after_score: Tuple[Column, ...] = ()
    extra_json_after_bboxes: Tuple[Column, ...] = ()
    extra_json_after_labels: Tuple[Column, ...] = ()
    extra_json_after_scores: Tuple[Column, ...] = ()


def _model_col(spec: BboxTaskInferenceSpec, suffix: str) -> str:
    return f"{spec.model_prefix}__{suffix}"


def run_bbox_task_inference(
    spec: BboxTaskInferenceSpec,
    *dfs,
    image__image_path__name: str,
    primary_keys: List[str],
    bbox_id__name: Optional[str],
    batch_size_default: int,
    model_primary_keys: List[str],
    inference_by_crops: bool = False,
    hCrossing: Optional[int] = None,
    vCrossing: Optional[int] = None,
    thresholdSpace: Optional[int] = None,
    threseholdSpace: Optional[int] = None,
    blockWidth: Optional[int] = None,
    blockHeight: Optional[int] = None,
    prediction_threshold: Optional[float] = None,
    class_name_to_threshold__name: Optional[str] = None,
) -> pd.DataFrame:
    threshold_space = resolve_threshold_space(
        threshold_space=thresholdSpace,
        thresehold_space=threseholdSpace,
    )
    df__image: pd.DataFrame = dfs[0]
    if len(dfs) >= 3:
        for df in dfs[1:-1]:
            df__image = pd.merge(df, df__image, on=primary_keys)
    df__model: pd.DataFrame = dfs[-1]
    model_other_primary_keys = [key for key in model_primary_keys if key not in primary_keys]
    if bbox_id__name is not None:
        columns = (
            primary_keys
            + model_other_primary_keys
            + [bbox_id__name, "x_min", "y_min", "x_max", "y_max"]
            + [column.name for column in spec.extra_bbox_id_after_coords]
            + ["label", "prediction__detection_score"]
            + [column.name for column in spec.extra_bbox_id_after_score]
        )
    else:
        columns = (
            primary_keys
            + model_other_primary_keys
            + ["bboxes"]
            + [column.name for column in spec.extra_json_after_bboxes]
            + ["labels", "prediction__detection_scores"]
            + [column.name for column in spec.extra_json_after_labels]
            + [column.name for column in spec.extra_json_after_scores]
        )
    if df__image.empty:
        return pd.DataFrame(columns=columns)

    image_paths_to_check = np.array(df__image[image__image_path__name])
    idxs = check_if_images_opens([str(x) for x in image_paths_to_check])
    df__image = df__image.loc[idxs].reset_index(drop=True)

    if len(set(df__image.columns).intersection(set(df__model.columns))) != 0:
        df__cross = pd.merge(df__image, df__model)
    else:
        df__cross = pd.merge(df__image, df__model, how="cross")

    score_threshold_col = _model_col(spec, "score_threshold")
    input_size_col = _model_col(spec, "input_size")
    model_path_col = _model_col(spec, "model_path")
    class_names_col = _model_col(spec, "class_names")
    type_col = _model_col(spec, "type")

    df__predictions = []
    for _, df_grouped in df__cross.groupby(model_primary_keys):
        row = df_grouped.iloc[0]
        model_spec = spec.get_model_spec(
            input_size=(int(row[input_size_col][0]), int(row[input_size_col][1])),
            model_path=row[model_path_col],
            class_names=tuple(str(class_name) for class_name in row[class_names_col]),
            model_type=row[type_col],
        )
        inferencer = PipelineModelSpec(
            detection_model_spec=model_spec,
            classification_model_spec=None,
        ).load_pipeline_inferencer()
        images_data = [ImageData(image_path=image_path) for image_path in df_grouped[image__image_path__name]]
        score_threshold = prediction_threshold if prediction_threshold is not None else row[score_threshold_col]
        if inference_by_crops:
            assert hCrossing is not None and vCrossing is not None
            assert threshold_space is not None and blockWidth is not None and blockHeight is not None
            if spec.crop_model_input_size_from_row:
                crop_input_size = (int(row[input_size_col][0]), int(row[input_size_col][1]))
            else:
                crop_input_size = spec.crop_model_input_size
            df_grouped["image_data"] = [
                predict_by_crops(
                    image_data=image_data,
                    inferencer=inferencer,
                    detection_score_threshold=score_threshold,
                    hCrossing=hCrossing,
                    vCrossing=vCrossing,
                    thresholdSpace=threshold_space,
                    blockWidth=blockWidth,
                    blockHeight=blockHeight,
                    model_input_size=crop_input_size,
                    include_masks=spec.crop_include_masks,
                    include_keypoints=spec.crop_include_keypoints,
                )
                for image_data in images_data
            ]
        else:
            df_grouped["image_data"] = inferencer.predict(
                images_data,
                detection_score_threshold=score_threshold,
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
        df__predictions.append(
            convert_df_with_image_data_to_df_with_bbox(
                df__with_image_data=df_grouped,
                primary_keys=primary_keys + model_other_primary_keys,
                bbox_id__name=bbox_id__name,
                image__image_path__name=image__image_path__name,
            )
        )

    if df__predictions:
        return pd.concat(df__predictions, ignore_index=True)[columns]
    return pd.DataFrame(columns=columns)


def make_bbox_inference_func(spec: BboxTaskInferenceSpec, *, inference_by_crops: bool = False):
    pk_kwarg = spec.model_primary_keys_kwarg

    def inference_func(*dfs, **kwargs):
        if pk_kwarg not in kwargs and "model_primary_keys" in kwargs:
            kwargs[pk_kwarg] = kwargs.pop("model_primary_keys")
        return run_bbox_task_inference(
            spec,
            *dfs,
            inference_by_crops=inference_by_crops,
            model_primary_keys=kwargs.pop(pk_kwarg),
            **kwargs,
        )

    return inference_func


def make_bbox_inference_using_thresholds_func(spec: BboxTaskInferenceSpec, base_inference_func):
    pk_kwarg = spec.model_primary_keys_kwarg

    def inference_using_thresholds_func(
        *dfs,
        image__image_path__name: str,
        primary_keys: List[str],
        bbox_id__name: str,
        batch_size_default: int,
        class_name_to_threshold__name: str,
        **kwargs,
    ):
        model_primary_keys = kwargs.pop(pk_kwarg, kwargs.pop("model_primary_keys"))
        df__model = pd.merge(dfs[-2], dfs[-1], on=model_primary_keys)
        dfs_list = list(dfs)[:-2] + [df__model]
        prediction_threshold = min_prediction_threshold_from_class_thresholds(
            df__model,
            class_name_to_threshold__name,
        )
        return base_inference_func(
            *dfs_list,
            image__image_path__name=image__image_path__name,
            primary_keys=primary_keys,
            bbox_id__name=bbox_id__name,
            batch_size_default=batch_size_default,
            prediction_threshold=prediction_threshold,
            class_name_to_threshold__name=class_name_to_threshold__name,
            **{pk_kwarg: model_primary_keys},
            **kwargs,
        )

    return inference_using_thresholds_func


def _register_bbox_inference_catalog(
    *,
    ds: DataStore,
    catalog: Catalog,
    output_name: str,
    dt__input__images,
    dt__input_model,
    primary_keys: List[str],
    bbox_id__name: Optional[str],
    create_table: bool,
    spec: BboxTaskInferenceSpec,
) -> None:
    if bbox_id__name is not None:
        extra_schema = [
            Column(bbox_id__name, String, primary_key=True),
            Column("x_min", Integer),
            Column("y_min", Integer),
            Column("x_max", Integer),
            Column("y_max", Integer),
            *spec.extra_bbox_id_after_coords,
            Column("label", String),
            Column("prediction__detection_score", Float),
            *spec.extra_bbox_id_after_score,
        ]
    else:
        extra_schema = [
            Column("bboxes", JSON),
            *spec.extra_json_after_bboxes,
            Column("labels", JSON),
            *spec.extra_json_after_labels,
            Column("prediction__detection_scores", JSON),
            *spec.extra_json_after_scores,
        ]
    catalog.add_datatable(
        output_name,
        Table(
            ds.get_or_create_table(
                output_name,
                TableStoreDB(
                    dbconn=ds.meta_dbconn,
                    name=output_name,
                    data_sql_schema=[
                        column for column in dt__input__images[0].primary_schema if column.name in primary_keys
                    ]
                    + [column for column in dt__input_model.primary_schema if column.name not in primary_keys]
                    + extra_schema,
                    create_table=create_table,
                ),
            ).table_store
        ),
    )


@dataclass
class BboxInferenceStepConfig:
    spec: BboxTaskInferenceSpec
    mode: InferenceMode
    input__image: PipelineInput | Sequence[PipelineInput]
    input__model: PipelineInput
    output__prediction: PipelineOutput
    primary_keys: List[str]
    chunk_size: int
    create_table: bool
    labels: Optional[Labels]
    image__image_path__name: str
    bbox_id__name: Optional[str]
    batch_size_default: int
    executor_config: Optional[ExecutorConfig]
    filters: Union[LabelDict, Callable[[], LabelDict], None]
    model_primary_keys: Optional[List[str]]
    prediction_threshold: Optional[float] = None
    class_name_to_threshold__name: str = "class_name_to_threshold"
    input__model_thresholds: Optional[PipelineInput] = None
    hCrossing: Optional[int] = None
    vCrossing: Optional[int] = None
    thresholdSpace: Optional[int] = None
    threseholdSpace: Optional[int] = None
    blockWidth: Optional[int] = None
    blockHeight: Optional[int] = None


def build_bbox_inference_compute(ds: DataStore, catalog: Catalog, config: BboxInferenceStepConfig) -> List[ComputeStep]:
    model_primary_keys = config.model_primary_keys or config.spec.default_model_primary_keys
    check_columns_are_in_table(ds, config.input__image, config.primary_keys)
    input__images = normalize_pipeline_inputs(config.input__image)
    input__image_names = [get_pipeline_input_name(input__image) for input__image in input__images]
    input_model_name = get_pipeline_input_name(config.input__model)
    output_name = get_pipeline_output_name(config.output__prediction)
    assert any(
        check_columns_are_in_table(ds, input__image, [config.image__image_path__name], raise_exc=False)
        for input__image in input__images
    )
    dt__input__images = [get_datatable(ds, input__image) for input__image in input__image_names]
    dt__input_model = get_datatable(ds, input_model_name)
    check_columns_are_in_table(ds, config.input__model, model_primary_keys + config.spec.model_columns)
    if config.mode == InferenceMode.THRESHOLDS:
        assert config.input__model_thresholds is not None
        check_columns_are_in_table(
            ds,
            config.input__model_thresholds,
            model_primary_keys + [config.class_name_to_threshold__name],
        )

    _register_bbox_inference_catalog(
        ds=ds,
        catalog=catalog,
        output_name=output_name,
        dt__input__images=dt__input__images,
        dt__input_model=dt__input_model,
        primary_keys=config.primary_keys,
        bbox_id__name=config.bbox_id__name,
        create_table=config.create_table,
        spec=config.spec,
    )

    pk_kwarg = config.spec.model_primary_keys_kwarg
    base_func = make_bbox_inference_func(config.spec, inference_by_crops=config.mode == InferenceMode.CROPS)
    if config.mode == InferenceMode.PLAIN:
        transform_func = base_func
        inputs = [*[required_pipeline_input(input__image) for input__image in input__images], config.input__model]
        kwargs = dict(
            primary_keys=config.primary_keys,
            image__image_path__name=config.image__image_path__name,
            bbox_id__name=config.bbox_id__name,
            batch_size_default=config.batch_size_default,
            prediction_threshold=config.prediction_threshold,
            **{pk_kwarg: model_primary_keys},
        )
    elif config.mode == InferenceMode.CROPS:
        transform_func = base_func
        inputs = [*[required_pipeline_input(input__image) for input__image in input__images], config.input__model]
        kwargs = dict(
            primary_keys=config.primary_keys,
            image__image_path__name=config.image__image_path__name,
            bbox_id__name=config.bbox_id__name,
            batch_size_default=config.batch_size_default,
            prediction_threshold=config.prediction_threshold,
            hCrossing=config.hCrossing,
            vCrossing=config.vCrossing,
            thresholdSpace=resolve_threshold_space(
                threshold_space=config.thresholdSpace,
                thresehold_space=config.threseholdSpace,
            ),
            blockWidth=config.blockWidth,
            blockHeight=config.blockHeight,
            **{pk_kwarg: model_primary_keys},
        )
    else:
        assert config.input__model_thresholds is not None
        model_thresholds = config.input__model_thresholds
        transform_func = make_bbox_inference_using_thresholds_func(config.spec, base_func)
        inputs = [
            *[required_pipeline_input(input__image) for input__image in input__images],
            config.input__model,
            model_thresholds,
        ]
        kwargs = dict(
            primary_keys=config.primary_keys,
            image__image_path__name=config.image__image_path__name,
            bbox_id__name=config.bbox_id__name,
            batch_size_default=config.batch_size_default,
            class_name_to_threshold__name=config.class_name_to_threshold__name,
            **{pk_kwarg: model_primary_keys},
        )

    pipeline = Pipeline(
        [
            BatchTransform(
                func=transform_func,
                inputs=inputs,
                outputs=[config.output__prediction],
                transform_keys=stable_unique(config.primary_keys + model_primary_keys),
                chunk_size=config.chunk_size,
                labels=config.labels,
                order_by=model_primary_keys,
                order="desc",
                kwargs=kwargs,
                executor_config=config.executor_config,
                filters=config.filters,
            ),
        ]
    )
    return build_compute(ds, catalog, pipeline)
