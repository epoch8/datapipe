from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Type, Union

import pandas as pd
from cv_pipeliner.metrics.detection import ImageDataMatching, get_df_detection_metrics
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
from datapipe.types import PipelineInput, PipelineOutput, IndexDF, Labels
from sqlalchemy import Column, Float
from sqlalchemy.sql.sqltypes import Integer, String

from datapipe_ml.core.datapipe import check_columns_are_in_table
from datapipe_ml.core.image_data import convert_df_with_bbox_to_df_with_image_data

KEYPOINTS_METRIC_COLUMNS = [
    "calc__images_support",
    "calc__support",
    "calc__TP",
    "calc__FP",
    "calc__FN",
    "calc__iou_mean",
    "calc__accuracy",
    "calc__precision",
    "calc__recall",
    "calc__f1_score",
    "calc__pose_P",
    "calc__pose_R",
    "calc__pose_mAP50",
    "calc__pose_mAP50_95",
]


def _resolve_training_data_yaml_path(df__keypoints_model: pd.DataFrame) -> Optional[str]:
    model_path = df__keypoints_model.iloc[0].get("keypoints_model__model_path")
    if model_path is None or pd.isna(model_path):
        return None
    yaml_path = Path(str(model_path)).parent.parent / "training_data.yaml"
    if not yaml_path.exists():
        return None
    return str(yaml_path)


def _empty_pose_metrics() -> Dict[str, Optional[float]]:
    return {
        "calc__pose_P": None,
        "calc__pose_R": None,
        "calc__pose_mAP50": None,
        "calc__pose_mAP50_95": None,
    }


def _float_or_none(value: Any) -> Optional[float]:
    return float(value) if value is not None else None


def _run_native_yolo_pose_validation(
    df__keypoints_model: pd.DataFrame,
    subset_id: str,
    yolo_validation_batch: int,
    yolo_validation_imgsz: Optional[int],
    yolo_validation_device: Optional[str],
) -> Dict[str, Optional[float]]:
    data_yaml_path = _resolve_training_data_yaml_path(df__keypoints_model=df__keypoints_model)
    if data_yaml_path is None:
        return _empty_pose_metrics()
    model_path = df__keypoints_model.iloc[0].get("keypoints_model__model_path")
    if model_path is None or pd.isna(model_path):
        return _empty_pose_metrics()
    try:
        import ultralytics

        model = ultralytics.YOLO(str(model_path))
        kwargs: Dict[str, Any] = {
            "data": data_yaml_path,
            "task": "pose",
            "split": subset_id,
            "batch": yolo_validation_batch,
            "verbose": False,
            "project": str(Path(data_yaml_path).parent),
        }
        if yolo_validation_imgsz is not None:
            kwargs["imgsz"] = yolo_validation_imgsz
        if yolo_validation_device is not None:
            kwargs["device"] = yolo_validation_device
        results = model.val(**kwargs)
        results_dict = getattr(results, "results_dict", {}) or {}
        return {
            "calc__pose_P": _float_or_none(results_dict.get("metrics/precision(P)")),
            "calc__pose_R": _float_or_none(results_dict.get("metrics/recall(P)")),
            "calc__pose_mAP50": _float_or_none(results_dict.get("metrics/mAP50(P)")),
            "calc__pose_mAP50_95": _float_or_none(results_dict.get("metrics/mAP50-95(P)")),
        }
    except Exception as exc:
        print(f"Native YOLO pose validation skipped due to error: {exc}")
        return _empty_pose_metrics()


def _derive_imgsz_from_keypoints_model(df__keypoints_model: pd.DataFrame) -> Optional[int]:
    if len(df__keypoints_model) == 0:
        return None
    input_size = df__keypoints_model.iloc[0].get("keypoints_model__input_size")
    if isinstance(input_size, (list, tuple)) and len(input_size) >= 1:
        try:
            return int(input_size[0])
        except Exception:
            return None
    return None


def count_keypoints_metrics_on_subset(
    df__image__ground_truth: pd.DataFrame,
    df__subset__has__image: pd.DataFrame,
    df__keypoints_model: pd.DataFrame,
    df__keypoints_prediction: pd.DataFrame,
    idx: IndexDF,
    primary_keys: List[str],
    minimum_iou: float,
    bbox_id__name: Optional[str],
    keypoints_model_primary_keys: List[str],
    image_data_matching_class: Type[ImageDataMatching],
    yolo_validation_batch: int,
    yolo_validation_imgsz: Optional[int],
    yolo_validation_device: Optional[str],
):
    if len(idx) not in [0, 1]:
        raise ValueError("Argument chunk_size must be 1 for this transformation")
    columns = keypoints_model_primary_keys + ["subset_id"] + KEYPOINTS_METRIC_COLUMNS
    if len(df__subset__has__image) == 0 or len(df__keypoints_prediction) == 0:
        return pd.DataFrame(columns=columns)

    subset_id = df__subset__has__image.iloc[0]["subset_id"]
    df__gt = pd.merge(df__image__ground_truth, df__subset__has__image)
    df__true_images_data = convert_df_with_bbox_to_df_with_image_data(df__gt, primary_keys, bbox_id__name)
    df__pred_images_data = convert_df_with_bbox_to_df_with_image_data(
        df__keypoints_prediction, primary_keys, bbox_id__name
    )
    df__images_data = pd.merge(df__true_images_data, df__pred_images_data, on=primary_keys, suffixes=("_true", "_pred"))
    df__metrics = get_df_detection_metrics(
        true_images_data=df__images_data["image_data_true"],
        pred_images_data=df__images_data["image_data_pred"],
        minimum_iou=minimum_iou,
        image_data_matching_class=image_data_matching_class,
    ).T.reset_index(drop=True)
    df__metrics.columns = KEYPOINTS_METRIC_COLUMNS[:10]
    for primary_key in keypoints_model_primary_keys:
        df__metrics[primary_key] = idx.iloc[0][primary_key]
    native_pose_metrics = _run_native_yolo_pose_validation(
        df__keypoints_model=df__keypoints_model,
        subset_id=subset_id,
        yolo_validation_batch=yolo_validation_batch,
        yolo_validation_imgsz=yolo_validation_imgsz,
        yolo_validation_device=yolo_validation_device,
    )
    for metric_name, metric_value in native_pose_metrics.items():
        df__metrics[metric_name] = metric_value
    df__metrics["subset_id"] = subset_id
    return df__metrics[columns]


@dataclass
class CountMetrics_Subset_KeypointsModel(PipelineStep):
    input__image__ground_truth: PipelineInput
    input__subset__has__image: PipelineInput
    input__keypoints_model: PipelineInput
    input__keypoints_prediction: PipelineInput
    output__keypoints_model__metrics__on__subset: PipelineOutput
    primary_keys: List[str]
    bbox_id__name: Optional[str] = "bbox_id"
    create_table: bool = False
    labels: Optional[Labels] = None
    minimum_iou: float = 0.5
    filters: Union[LabelDict, Callable[[], LabelDict], None] = None
    keypoints_model_primary_keys: Optional[List[str]] = None
    executor_config: Optional[ExecutorConfig] = None
    image_data_matching_class: Type[ImageDataMatching] = ImageDataMatching
    yolo_validation_batch: int = 8
    yolo_validation_imgsz: Optional[int] = None
    yolo_validation_device: Optional[str] = None

    def build_compute(self, ds: DataStore, catalog: Catalog) -> List[ComputeStep]:
        if self.keypoints_model_primary_keys is None:
            self.keypoints_model_primary_keys = ["keypoints_model_id"]
        check_columns_are_in_table(ds, self.input__subset__has__image, ["subset_id"])
        if self.bbox_id__name is not None:
            check_columns_are_in_table(
                ds,
                self.input__image__ground_truth,
                self.primary_keys + [self.bbox_id__name, "x_min", "y_min", "x_max", "y_max", "keypoints"],
            )
            check_columns_are_in_table(
                ds,
                self.input__keypoints_prediction,
                self.primary_keys
                + self.keypoints_model_primary_keys
                + [self.bbox_id__name, "x_min", "y_min", "x_max", "y_max", "keypoints"],
            )
        else:
            check_columns_are_in_table(ds, self.input__image__ground_truth, self.primary_keys + ["bboxes", "keypoints"])
            check_columns_are_in_table(
                ds,
                self.input__keypoints_prediction,
                self.primary_keys + self.keypoints_model_primary_keys + ["bboxes", "keypoints"],
            )
        check_columns_are_in_table(
            ds,
            self.input__keypoints_model,
            self.keypoints_model_primary_keys + ["keypoints_model__input_size", "keypoints_model__model_path"],
        )
        dt__pred = ds.get_table(self.input__keypoints_prediction)
        catalog.add_datatable(
            self.output__keypoints_model__metrics__on__subset,
            Table(
                ds.get_or_create_table(
                    self.output__keypoints_model__metrics__on__subset,
                    TableStoreDB(
                        dbconn=ds.meta_dbconn,
                        name=self.output__keypoints_model__metrics__on__subset,
                        data_sql_schema=[
                            column
                            for column in dt__pred.primary_schema
                            if column.name in self.keypoints_model_primary_keys
                        ]
                        + [
                            Column("subset_id", String, primary_key=True),
                            *[
                                Column(
                                    column_name,
                                    (
                                        Float
                                        if "iou" in column_name
                                        or "accuracy" in column_name
                                        or "precision" in column_name
                                        or "recall" in column_name
                                        or "f1" in column_name
                                        or "pose" in column_name
                                        else Integer
                                    ),
                                )
                                for column_name in KEYPOINTS_METRIC_COLUMNS
                            ],
                        ],
                        create_table=self.create_table,
                    ),
                ).table_store
            ),
        )
        pipeline = Pipeline(
            [
                BatchTransform(
                    func=count_keypoints_metrics_on_subset,
                    inputs=[
                        self.input__image__ground_truth,
                        self.input__subset__has__image,
                        self.input__keypoints_model,
                        self.input__keypoints_prediction,
                    ],
                    outputs=[self.output__keypoints_model__metrics__on__subset],
                    transform_keys=self.keypoints_model_primary_keys + ["subset_id"],
                    executor_config=self.executor_config,
                    labels=self.labels,
                    kwargs=dict(
                        primary_keys=self.primary_keys,
                        minimum_iou=self.minimum_iou,
                        bbox_id__name=self.bbox_id__name,
                        keypoints_model_primary_keys=self.keypoints_model_primary_keys,
                        image_data_matching_class=self.image_data_matching_class,
                        yolo_validation_batch=self.yolo_validation_batch,
                        yolo_validation_imgsz=self.yolo_validation_imgsz,
                        yolo_validation_device=self.yolo_validation_device,
                    ),
                    chunk_size=1,
                    filters=self.filters,
                ),
            ]
        )
        return build_compute(ds, catalog, pipeline)


def _check_model_id_consistency(
    df__keypoints_model: pd.DataFrame,
    df__keypoints_prediction: pd.DataFrame,
    keypoints_model_primary_keys: List[str],
):
    pred_ids = df__keypoints_prediction[keypoints_model_primary_keys].drop_duplicates()
    model_ids = df__keypoints_model[keypoints_model_primary_keys].drop_duplicates()
    joined = pd.merge(pred_ids, model_ids, on=keypoints_model_primary_keys, how="inner")
    if len(joined) != len(pred_ids):
        raise ValueError("Model ID mismatch: prediction table has model IDs not present in keypoints model table.")


def count_keypoints_metrics_on_frozen_dataset(
    df__keypoints_frozen_dataset__has__image_gt: pd.DataFrame,
    df__keypoints_model: pd.DataFrame,
    df__keypoints_prediction: pd.DataFrame,
    idx: IndexDF,
    primary_keys: List[str],
    minimum_iou: float,
    bbox_id__name: Optional[str],
    keypoints_model_primary_keys: List[str],
    keypoints_frozen_dataset_id__name: str,
    image_data_matching_class: Type[ImageDataMatching],
    yolo_validation_batch: int,
    yolo_validation_device: Optional[str],
):
    columns = keypoints_model_primary_keys + [keypoints_frozen_dataset_id__name, "subset_id"] + KEYPOINTS_METRIC_COLUMNS
    if len(idx) not in [0, 1]:
        raise ValueError("Argument chunk_size must be 1 for this transformation")
    if len(df__keypoints_frozen_dataset__has__image_gt) == 0 or len(df__keypoints_prediction) == 0:
        return pd.DataFrame(columns=columns)
    _check_model_id_consistency(
        df__keypoints_model=df__keypoints_model,
        df__keypoints_prediction=df__keypoints_prediction,
        keypoints_model_primary_keys=keypoints_model_primary_keys,
    )
    yolo_validation_imgsz = _derive_imgsz_from_keypoints_model(df__keypoints_model=df__keypoints_model)
    df__metrics = count_keypoints_metrics_on_subset(
        df__image__ground_truth=df__keypoints_frozen_dataset__has__image_gt.drop(
            columns=[keypoints_frozen_dataset_id__name, "subset_id"]
        ),
        df__subset__has__image=df__keypoints_frozen_dataset__has__image_gt[
            primary_keys + ["subset_id"]
        ].drop_duplicates(),
        df__keypoints_model=df__keypoints_model,
        df__keypoints_prediction=df__keypoints_prediction,
        idx=idx,
        primary_keys=primary_keys,
        minimum_iou=minimum_iou,
        bbox_id__name=bbox_id__name,
        keypoints_model_primary_keys=keypoints_model_primary_keys,
        image_data_matching_class=image_data_matching_class,
        yolo_validation_batch=yolo_validation_batch,
        yolo_validation_imgsz=yolo_validation_imgsz,
        yolo_validation_device=yolo_validation_device,
    )
    df__metrics[keypoints_frozen_dataset_id__name] = df__keypoints_frozen_dataset__has__image_gt.iloc[0][
        keypoints_frozen_dataset_id__name
    ]
    return df__metrics[columns]


@dataclass
class CountMetrics_FrozenDataset_KeypointsModel(PipelineStep):
    input__keypoints_frozen_dataset__has__image_gt: PipelineInput
    input__keypoints_model: PipelineInput
    input__keypoints_prediction: PipelineInput
    output__keypoints_model__metrics_on__frozen_dataset: PipelineOutput
    primary_keys: List[str]
    bbox_id__name: Optional[str] = "bbox_id"
    create_table: bool = False
    labels: Optional[Labels] = None
    minimum_iou: float = 0.5
    filters: Union[LabelDict, Callable[[], LabelDict], None] = None
    keypoints_model_primary_keys: Optional[List[str]] = None
    keypoints_frozen_dataset_id__name: str = "keypoints_frozen_dataset_id"
    executor_config: Optional[ExecutorConfig] = None
    image_data_matching_class: Type[ImageDataMatching] = ImageDataMatching
    yolo_validation_batch: int = 8
    yolo_validation_device: Optional[str] = None

    def build_compute(self, ds: DataStore, catalog: Catalog) -> List[ComputeStep]:
        if self.keypoints_model_primary_keys is None:
            self.keypoints_model_primary_keys = ["keypoints_model_id"]
        check_columns_are_in_table(
            ds,
            self.input__keypoints_model,
            self.keypoints_model_primary_keys + ["keypoints_model__input_size", "keypoints_model__model_path"],
        )
        required_gt = self.primary_keys + [self.keypoints_frozen_dataset_id__name, "subset_id"]
        required_pred = self.primary_keys + self.keypoints_model_primary_keys
        if self.bbox_id__name is not None:
            required_gt += [self.bbox_id__name, "x_min", "y_min", "x_max", "y_max", "keypoints"]
            required_pred += [self.bbox_id__name, "x_min", "y_min", "x_max", "y_max", "keypoints"]
        else:
            required_gt += ["bboxes", "keypoints"]
            required_pred += ["bboxes", "keypoints"]
        check_columns_are_in_table(ds, self.input__keypoints_frozen_dataset__has__image_gt, required_gt)
        check_columns_are_in_table(ds, self.input__keypoints_prediction, required_pred)

        dt__pred = ds.get_table(self.input__keypoints_prediction)
        catalog.add_datatable(
            self.output__keypoints_model__metrics_on__frozen_dataset,
            Table(
                ds.get_or_create_table(
                    self.output__keypoints_model__metrics_on__frozen_dataset,
                    TableStoreDB(
                        dbconn=ds.meta_dbconn,
                        name=self.output__keypoints_model__metrics_on__frozen_dataset,
                        data_sql_schema=[
                            column
                            for column in dt__pred.primary_schema
                            if column.name in self.keypoints_model_primary_keys
                        ]
                        + [
                            Column(self.keypoints_frozen_dataset_id__name, String, primary_key=True),
                            Column("subset_id", String, primary_key=True),
                            *[
                                Column(
                                    column_name,
                                    (
                                        Float
                                        if "iou" in column_name
                                        or "accuracy" in column_name
                                        or "precision" in column_name
                                        or "recall" in column_name
                                        or "f1" in column_name
                                        or "pose" in column_name
                                        else Integer
                                    ),
                                )
                                for column_name in KEYPOINTS_METRIC_COLUMNS
                            ],
                        ],
                        create_table=self.create_table,
                    ),
                ).table_store
            ),
        )
        pipeline = Pipeline(
            [
                BatchTransform(
                    func=count_keypoints_metrics_on_frozen_dataset,
                    inputs=[
                        self.input__keypoints_frozen_dataset__has__image_gt,
                        self.input__keypoints_model,
                        self.input__keypoints_prediction,
                    ],
                    outputs=[self.output__keypoints_model__metrics_on__frozen_dataset],
                    transform_keys=self.keypoints_model_primary_keys
                    + ["subset_id", self.keypoints_frozen_dataset_id__name],
                    executor_config=self.executor_config,
                    labels=self.labels,
                    kwargs=dict(
                        primary_keys=self.primary_keys,
                        minimum_iou=self.minimum_iou,
                        bbox_id__name=self.bbox_id__name,
                        keypoints_model_primary_keys=self.keypoints_model_primary_keys,
                        keypoints_frozen_dataset_id__name=self.keypoints_frozen_dataset_id__name,
                        image_data_matching_class=self.image_data_matching_class,
                        yolo_validation_batch=self.yolo_validation_batch,
                        yolo_validation_device=self.yolo_validation_device,
                    ),
                    chunk_size=1,
                    filters=self.filters,
                ),
            ]
        )
        return build_compute(ds, catalog, pipeline)
