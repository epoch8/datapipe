from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Type, Union

import numpy as np
import pandas as pd
from cv_pipeliner.metrics.detection import ImageDataMatching, get_df_detection_metrics
from cv_pipeliner.metrics.keypoints import get_df_keypoints_metrics
from datapipe.compute import (
    Catalog,
    ComputeStep,
    Pipeline,
    PipelineStep,
    Table,
    build_compute,
)
from datapipe.datatable import DataStore, DataTable
from datapipe.executor import ExecutorConfig
from datapipe.run_config import LabelDict, RunConfig
from datapipe.step.batch_transform import BatchTransform, DatatableBatchTransform
from datapipe.store.database import TableStoreDB
from datapipe.types import PipelineInput, PipelineOutput, IndexDF, Labels, required_pipeline_input
from sqlalchemy import Column, Float
from sqlalchemy import cast as sql_cast
from sqlalchemy import func, select, tuple_
from sqlalchemy.sql.sqltypes import Integer, String

from datapipe_ml.core.datapipe import (
    check_columns_are_in_table,
    get_datatable,
    get_pipeline_table_name,
    pipeline_output_as_input,
)
from datapipe_ml.core.image_data import convert_df_with_bbox_to_df_with_image_data
from datapipe_ml.metrics.common import (
    KNOWN_OVERALL_METRIC_COLUMNS,
    METRICS_NULL_LABEL,
    OVERALL_BASE_METRIC_COLUMNS,
    float_columns,
    is_metrics_null_label,
    stable_unique,
)
from datapipe_ml.metrics.inputs import (
    build_ground_truth_batch_inputs,
    get_ground_truth_datatables,
    ground_truth_convert_keys,
    merged_ground_truth_primary_schema,
    model_primary_key_columns,
    model_primary_keys_in_table,
    prediction_convert_keys,
    primary_ground_truth_input,
    wrap_ground_truth_inputs,
)
from datapipe_ml.workflows.detection_classification.metrics import (
    count_pipeline_metrics_on_image,
    count_pipeline_metrics_on_subset,
)

POSE_SUPPORT_COLUMN = "calc__pose_support"
POSE_VALUE_COLUMNS = [
    "calc__pose_P",
    "calc__pose_R",
    "calc__pose_mAP50",
    "calc__pose_mAP50_95",
]
POSE_METRIC_COLUMNS = [POSE_SUPPORT_COLUMN] + POSE_VALUE_COLUMNS

FROZEN_DETECTION_METRIC_COLUMNS = [
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
]
FROZEN_METRIC_COLUMNS = FROZEN_DETECTION_METRIC_COLUMNS + POSE_METRIC_COLUMNS


def _pose_metrics_for_images(
    df__image__ground_truth: pd.DataFrame,
    df__subset__has__image: pd.DataFrame,
    df__keypoints_prediction: pd.DataFrame,
    primary_keys: List[str],
    bbox_id__name: Optional[str],
    keypoints_model_primary_keys: List[str],
    class_names: Optional[List[str]],
    sigma: Optional[np.ndarray],
    subset_ids: Optional[List[str]],
) -> pd.DataFrame:
    """One pose-metrics row per (primary_keys + model_keys + subset_id)."""
    df__subset = df__subset__has__image
    if subset_ids is not None:
        df__subset = df__subset[df__subset["subset_id"].isin(subset_ids)]
    key_cols = stable_unique(primary_keys + keypoints_model_primary_keys + ["subset_id"])
    empty = pd.DataFrame(columns=key_cols + POSE_METRIC_COLUMNS)
    if len(df__subset) == 0 or len(df__keypoints_prediction) == 0:
        return empty

    df__gt = pd.merge(df__image__ground_truth, df__subset)
    gt_convert_keys = ground_truth_convert_keys(primary_keys, keypoints_model_primary_keys, df__gt)
    df__true = convert_df_with_bbox_to_df_with_image_data(df__gt, gt_convert_keys, bbox_id__name)
    df__pred = pd.merge(df__keypoints_prediction, df__subset)
    pred_convert_keys = prediction_convert_keys(primary_keys, keypoints_model_primary_keys, df__pred)
    df__pred_img = convert_df_with_bbox_to_df_with_image_data(df__pred, pred_convert_keys, bbox_id__name)
    df__images = pd.merge(df__true, df__pred_img, on=primary_keys + ["subset_id"], suffixes=("_true", "_pred"))
    if len(df__images) == 0:
        return empty

    rows = []
    for idx in df__images.index:
        df__pose = get_df_keypoints_metrics(
            true_images_data=[df__images.loc[idx, "image_data_true"]],
            pred_images_data=[df__images.loc[idx, "image_data_pred"]],
            class_names=class_names,
            sigma=sigma,
        ).T.reset_index(drop=True)
        # images_support / support from get_df_keypoints_metrics: support = GT instances with keypoints
        df__pose.columns = [
            "calc__images_support",
            POSE_SUPPORT_COLUMN,
            *POSE_VALUE_COLUMNS,
        ]
        row = {col: df__pose.iloc[0][col] for col in POSE_METRIC_COLUMNS}
        for key in key_cols:
            row[key] = df__images.loc[idx, key]
        rows.append(row)
    return pd.DataFrame(rows, columns=key_cols + POSE_METRIC_COLUMNS)


def count_keypoints_metrics_on_image(
    df__image__ground_truth: pd.DataFrame,
    df__subset__has__image: pd.DataFrame,
    df__keypoints_prediction: pd.DataFrame,
    primary_keys: List[str],
    minimum_iou: float,
    bbox_id__name: Optional[str],
    keypoints_model_primary_keys: List[str],
    count_by_thresholds: Optional[List[float]],
    image_data_matching_class: Type[ImageDataMatching],
    label_mapper: Optional[Dict[str, str]],
    subset_ids: Optional[List[str]] = None,
    class_names: Optional[List[str]] = None,
    sigma: Optional[np.ndarray] = None,
):
    """Pipeline per-image metrics (base table) + pose OKS metrics on overall (null-label) rows."""
    df_base = count_pipeline_metrics_on_image(
        df__image__ground_truth=df__image__ground_truth,
        df__subset__has__image=df__subset__has__image,
        df__pipeline_prediction=df__keypoints_prediction,
        primary_keys=primary_keys,
        minimum_iou=minimum_iou,
        bbox_id__name=bbox_id__name,
        pipeline_model_primary_keys=keypoints_model_primary_keys,
        count_by_thresholds=count_by_thresholds,
        image_data_matching_class=image_data_matching_class,
        label_mapper=label_mapper,
        subset_ids=subset_ids,
    )
    for col in POSE_METRIC_COLUMNS:
        df_base[col] = pd.NA

    if len(df_base) == 0:
        return df_base

    df_pose = _pose_metrics_for_images(
        df__image__ground_truth=df__image__ground_truth,
        df__subset__has__image=df__subset__has__image,
        df__keypoints_prediction=df__keypoints_prediction,
        primary_keys=primary_keys,
        bbox_id__name=bbox_id__name,
        keypoints_model_primary_keys=keypoints_model_primary_keys,
        class_names=class_names,
        sigma=sigma,
        subset_ids=subset_ids,
    )
    if len(df_pose) == 0:
        return df_base

    key_cols = stable_unique(primary_keys + keypoints_model_primary_keys + ["subset_id"])
    null_mask = df_base["label"].map(is_metrics_null_label)
    df_null = df_base.loc[null_mask].drop(columns=POSE_METRIC_COLUMNS, errors="ignore")
    df_null = df_null.merge(df_pose, on=key_cols, how="left")
    df_base = pd.concat([df_base.loc[~null_mask], df_null], ignore_index=True)
    return df_base


def _aggregate_pose_on_subset(
    ds: DataStore,
    idx: IndexDF,
    input_dts: List[DataTable],
    keypoints_model_primary_keys: List[str],
    count_by_thresholds: bool,
) -> pd.DataFrame:
    """Aggregate pose metrics from overall (null-label) rows, weighted by pose_support."""
    dt = input_dts[0]
    assert isinstance(dt.table_store, TableStoreDB)
    tbl = dt.table_store.data_table

    col_subset = tbl.c["subset_id"]
    col_label = tbl.c["label"]
    col_pose_support = tbl.c[POSE_SUPPORT_COLUMN]
    col_thr = tbl.c["threshold"] if count_by_thresholds and "threshold" in tbl.c else None

    group_cols = [tbl.c[k] for k in keypoints_model_primary_keys] + [col_subset]
    if col_thr is not None:
        group_cols.append(col_thr)

    support_f = sql_cast(col_pose_support, Float)
    support_sum_f = func.nullif(sql_cast(func.sum(col_pose_support), Float), sql_cast(0, Float))
    zero = sql_cast(0, Float)

    def _weighted_mean(col):
        return func.coalesce(func.sum(sql_cast(col, Float) * support_f) / support_sum_f, zero)

    select_cols = [
        *[tbl.c[k] for k in keypoints_model_primary_keys],
        col_subset.label("subset_id"),
    ]
    if col_thr is not None:
        select_cols.append(col_thr.label("threshold"))
    select_cols.append(func.sum(col_pose_support).label(POSE_SUPPORT_COLUMN))
    select_cols.extend([_weighted_mean(tbl.c[col]).label(col) for col in POSE_VALUE_COLUMNS])

    stmt = (
        select(*select_cols)
        .select_from(tbl)
        .where(tuple_(*([tbl.c[k] for k in idx.columns])).in_(list(idx.itertuples(index=False, name=None))))
        .where(col_label == METRICS_NULL_LABEL)
        .group_by(*group_cols)
    )

    with ds.meta_dbconn.con.begin() as con:
        df = pd.read_sql(stmt, con)

    merge_keys = keypoints_model_primary_keys + ["subset_id"] + (["threshold"] if col_thr is not None else [])
    ordered = merge_keys + POSE_METRIC_COLUMNS
    if df.empty:
        return pd.DataFrame(columns=ordered)
    return df[ordered]


def count_keypoints_metrics_on_subset(
    ds: DataStore,
    idx: IndexDF,
    input_dts: List[DataTable],
    run_config: Optional[RunConfig] = None,
    kwargs: Optional[Dict[str, Any]] = None,
):
    """Pipeline subset aggregation (by-cls + overall) with pose columns on overall."""
    kwargs = kwargs or {}
    keypoints_model_primary_keys: List[str] = kwargs["keypoints_model_primary_keys"]
    has_threshold = bool(kwargs.get("has_threshold", False))
    pipeline_kwargs = {
        **kwargs,
        "pipeline_model_primary_keys": keypoints_model_primary_keys,
    }
    df_by_cls, df_overall = count_pipeline_metrics_on_subset(
        ds, idx, input_dts, run_config=run_config, kwargs=pipeline_kwargs
    )

    df_pose = _aggregate_pose_on_subset(
        ds,
        idx,
        input_dts,
        keypoints_model_primary_keys=keypoints_model_primary_keys,
        count_by_thresholds=has_threshold,
    )
    merge_keys = keypoints_model_primary_keys + ["subset_id"]
    if has_threshold and "threshold" in df_overall.columns:
        merge_keys = merge_keys + ["threshold"]

    if len(df_overall) == 0:
        for col in POSE_METRIC_COLUMNS:
            df_overall[col] = pd.Series(dtype=float)
    else:
        df_overall = df_overall.drop(columns=[c for c in POSE_METRIC_COLUMNS if c in df_overall.columns])
        df_overall = df_overall.merge(df_pose, on=merge_keys, how="left")

    return df_by_cls, df_overall


@dataclass
class CountMetrics_Subset_KeypointsModel(PipelineStep):
    """
    Same shape as CountMetrics_Subset_PipelineModel, with pose OKS metrics attached
    to overall (null-label) per-image rows and to the overall subset table.
    """

    input__image__ground_truth: PipelineInput | Sequence[PipelineInput]
    input__subset__has__image: PipelineInput
    input__keypoints_prediction: PipelineInput
    output__keypoints_model__metrics_on__image: PipelineOutput
    output__keypoints_model__metrics_by_cls_on__subset: PipelineOutput
    output__keypoints_model__metrics_on__subset: PipelineOutput
    primary_keys: List[str]
    input__keypoints_model: Optional[PipelineInput] = None  # API compat; unused
    bbox_id__name: Optional[str] = "bbox_id"
    create_table: bool = False
    labels: Optional[Labels] = None
    minimum_iou: float = 0.5
    chunk_size: int = 100
    pseudo_class_names: List[str] = field(default_factory=list)
    known_class_names: Optional[List[str]] = None
    filters: Union[LabelDict, Callable[[], LabelDict], None] = None
    keypoints_model_primary_keys: Optional[List[str]] = None
    count_by_thresholds: Optional[List[float]] = None
    subset_ids: Optional[List[str]] = None
    executor_config: Optional[ExecutorConfig] = None
    image_data_matching_class: Type[ImageDataMatching] = ImageDataMatching
    label_mapper: Optional[Dict[str, str]] = None
    class_names: Optional[List[str]] = None
    sigma: Optional[np.ndarray] = None

    def build_compute(self, ds: DataStore, catalog: Catalog) -> List[ComputeStep]:
        if self.count_by_thresholds is not None and not all(0.0 < t < 1.0 for t in self.count_by_thresholds):
            raise ValueError("count_by_thresholds must contain values between 0.0 and 1.0")
        if self.keypoints_model_primary_keys is None:
            self.keypoints_model_primary_keys = ["keypoints_model_id"]
        if self.filters is not None and not isinstance(self.filters, dict) and not callable(self.filters):
            raise TypeError("filters must be a dict or a zero-argument callable returning a dict")

        check_columns_are_in_table(ds, self.input__subset__has__image, ["subset_id"])
        primary_gt = primary_ground_truth_input(self.input__image__ground_truth)
        prediction_model_keys = model_primary_keys_in_table(
            ds, self.input__keypoints_prediction, self.keypoints_model_primary_keys
        )
        check_columns_are_in_table(ds, self.input__image__ground_truth, self.primary_keys)
        if self.bbox_id__name is not None:
            check_columns_are_in_table(
                ds,
                primary_gt,
                self.primary_keys + [self.bbox_id__name, "x_min", "y_min", "x_max", "y_max", "label", "keypoints"],
            )
            check_columns_are_in_table(
                ds,
                self.input__keypoints_prediction,
                self.primary_keys
                + prediction_model_keys
                + [self.bbox_id__name, "x_min", "y_min", "x_max", "y_max", "label", "keypoints"],
            )
        else:
            check_columns_are_in_table(ds, primary_gt, self.primary_keys + ["bboxes", "labels", "keypoints"])
            check_columns_are_in_table(
                ds,
                self.input__keypoints_prediction,
                self.primary_keys + prediction_model_keys + ["bboxes", "labels", "keypoints"],
            )

        dt__gt_tables = get_ground_truth_datatables(ds, self.input__image__ground_truth)
        dt__pred = get_datatable(ds, self.input__keypoints_prediction)
        model_key_columns = model_primary_key_columns(
            ds,
            self.keypoints_model_primary_keys,
            prediction=self.input__keypoints_prediction,
            ground_truth=self.input__image__ground_truth,
        )

        catalog.add_datatable(
            get_pipeline_table_name(self.output__keypoints_model__metrics_on__image),
            Table(
                ds.get_or_create_table(
                    get_pipeline_table_name(self.output__keypoints_model__metrics_on__image),
                    TableStoreDB(
                        dbconn=ds.meta_dbconn,
                        name=get_pipeline_table_name(self.output__keypoints_model__metrics_on__image),
                        data_sql_schema=merged_ground_truth_primary_schema(dt__gt_tables)
                        + [column for column in dt__pred.primary_schema if column.name not in self.primary_keys]
                        + [
                            Column("subset_id", String, primary_key=True),
                            Column("label", String, primary_key=True),
                        ]
                        + ([Column("threshold", Float, primary_key=True)] if self.count_by_thresholds else [])
                        + [
                            Column("calc__images_support", Integer),
                            Column("calc__support", Integer),
                            Column("calc__TP", Integer),
                            Column("calc__FP", Integer),
                            Column("calc__FN", Integer),
                            Column("calc__TP_extra_bbox", Integer),
                            Column("calc__FP_extra_bbox", Integer),
                            Column("calc__FN_extra_bbox", Integer),
                            Column(POSE_SUPPORT_COLUMN, Integer),
                            *[Column(name, Float) for name in POSE_VALUE_COLUMNS],
                        ],
                        create_table=self.create_table,
                    ),
                ).table_store
            ),
        )
        catalog.add_datatable(
            get_pipeline_table_name(self.output__keypoints_model__metrics_by_cls_on__subset),
            Table(
                ds.get_or_create_table(
                    get_pipeline_table_name(self.output__keypoints_model__metrics_by_cls_on__subset),
                    TableStoreDB(
                        dbconn=ds.meta_dbconn,
                        name=get_pipeline_table_name(self.output__keypoints_model__metrics_by_cls_on__subset),
                        data_sql_schema=model_key_columns
                        + [
                            Column("subset_id", String, primary_key=True),
                            Column("label", String, primary_key=True),
                        ]
                        + ([Column("threshold", Float, primary_key=True)] if self.count_by_thresholds else [])
                        + [
                            Column("calc__images_support", Integer),
                            Column("calc__support", Integer),
                            Column("calc__TP", Integer),
                            Column("calc__FP", Integer),
                            Column("calc__FN", Integer),
                            Column("calc__precision", Float),
                            Column("calc__recall", Float),
                            Column("calc__f1_score", Float),
                        ],
                        create_table=self.create_table,
                    ),
                ).table_store
            ),
        )
        extra_known = float_columns(KNOWN_OVERALL_METRIC_COLUMNS) if self.known_class_names else []
        catalog.add_datatable(
            get_pipeline_table_name(self.output__keypoints_model__metrics_on__subset),
            Table(
                ds.get_or_create_table(
                    get_pipeline_table_name(self.output__keypoints_model__metrics_on__subset),
                    TableStoreDB(
                        dbconn=ds.meta_dbconn,
                        name=get_pipeline_table_name(self.output__keypoints_model__metrics_on__subset),
                        data_sql_schema=model_key_columns
                        + [Column("subset_id", String, primary_key=True)]
                        + ([Column("threshold", Float, primary_key=True)] if self.count_by_thresholds else [])
                        + [
                            Column("calc__images_support", Integer),
                            Column("calc__support", Integer),
                            *float_columns(OVERALL_BASE_METRIC_COLUMNS[2:]),
                        ]
                        + extra_known
                        + [Column(POSE_SUPPORT_COLUMN, Integer)]
                        + [Column(name, Float) for name in POSE_VALUE_COLUMNS],
                        create_table=self.create_table,
                    ),
                ).table_store
            ),
        )

        ground_truth_inputs = build_ground_truth_batch_inputs(self.input__image__ground_truth)
        pipeline = Pipeline(
            [
                BatchTransform(
                    func=wrap_ground_truth_inputs(
                        count_keypoints_metrics_on_image,
                        n_ground_truth_inputs=len(ground_truth_inputs),
                        primary_keys=self.primary_keys,
                    ),
                    inputs=[
                        *ground_truth_inputs,
                        required_pipeline_input(self.input__subset__has__image),
                        required_pipeline_input(self.input__keypoints_prediction),
                    ],
                    outputs=[self.output__keypoints_model__metrics_on__image],
                    transform_keys=sorted(
                        set(self.primary_keys + self.keypoints_model_primary_keys + ["subset_id"])
                    ),
                    executor_config=self.executor_config,
                    labels=self.labels,
                    kwargs=dict(
                        primary_keys=self.primary_keys,
                        minimum_iou=self.minimum_iou,
                        bbox_id__name=self.bbox_id__name,
                        keypoints_model_primary_keys=self.keypoints_model_primary_keys,
                        count_by_thresholds=self.count_by_thresholds,
                        image_data_matching_class=self.image_data_matching_class,
                        label_mapper=self.label_mapper,
                        subset_ids=self.subset_ids,
                        class_names=self.class_names,
                        sigma=self.sigma,
                    ),
                    chunk_size=self.chunk_size,
                    filters=self.filters,
                ),
                DatatableBatchTransform(
                    func=count_keypoints_metrics_on_subset,
                    inputs=[pipeline_output_as_input(self.output__keypoints_model__metrics_on__image)],
                    outputs=[
                        self.output__keypoints_model__metrics_by_cls_on__subset,
                        self.output__keypoints_model__metrics_on__subset,
                    ],
                    transform_keys=self.keypoints_model_primary_keys
                    + ["subset_id"]
                    + (["threshold"] if self.count_by_thresholds else []),
                    labels=self.labels,
                    kwargs=dict(
                        keypoints_model_primary_keys=self.keypoints_model_primary_keys,
                        primary_keys=self.primary_keys,
                        pseudo_class_names=self.pseudo_class_names,
                        known_class_names=self.known_class_names,
                        has_threshold=self.count_by_thresholds is not None,
                    ),
                    chunk_size=1,
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


def _float_column_type(column_name: str):
    if column_name.endswith("_support"):
        return Integer
    return (
        Float
        if (
            "iou" in column_name
            or "accuracy" in column_name
            or "precision" in column_name
            or "recall" in column_name
            or "f1" in column_name
            or "pose" in column_name
        )
        else Integer
    )


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
    class_names: Optional[List[str]] = None,
    sigma: Optional[np.ndarray] = None,
):
    columns = (
        keypoints_model_primary_keys
        + [keypoints_frozen_dataset_id__name, "subset_id"]
        + FROZEN_METRIC_COLUMNS
    )
    if len(idx) not in [0, 1]:
        raise ValueError("Argument chunk_size must be 1 for this transformation")
    if len(df__keypoints_frozen_dataset__has__image_gt) == 0 or len(df__keypoints_prediction) == 0:
        return pd.DataFrame(columns=columns)
    _check_model_id_consistency(
        df__keypoints_model=df__keypoints_model,
        df__keypoints_prediction=df__keypoints_prediction,
        keypoints_model_primary_keys=keypoints_model_primary_keys,
    )

    frozen_id = df__keypoints_frozen_dataset__has__image_gt.iloc[0][keypoints_frozen_dataset_id__name]
    subset_id = df__keypoints_frozen_dataset__has__image_gt.iloc[0]["subset_id"]
    df__gt = df__keypoints_frozen_dataset__has__image_gt.drop(
        columns=[keypoints_frozen_dataset_id__name], errors="ignore"
    )
    gt_convert_keys = ground_truth_convert_keys(primary_keys, keypoints_model_primary_keys, df__gt)
    df__true = convert_df_with_bbox_to_df_with_image_data(df__gt, gt_convert_keys, bbox_id__name)
    df__pred = pd.merge(
        df__keypoints_prediction,
        df__keypoints_frozen_dataset__has__image_gt[primary_keys + ["subset_id"]].drop_duplicates(),
    )
    pred_convert_keys = prediction_convert_keys(primary_keys, keypoints_model_primary_keys, df__pred)
    df__pred_img = convert_df_with_bbox_to_df_with_image_data(df__pred, pred_convert_keys, bbox_id__name)
    df__images = pd.merge(df__true, df__pred_img, on=primary_keys + ["subset_id"], suffixes=("_true", "_pred"))
    if len(df__images) == 0:
        return pd.DataFrame(columns=columns)

    true_images = list(df__images["image_data_true"])
    pred_images = list(df__images["image_data_pred"])
    df__detection = get_df_detection_metrics(
        true_images_data=true_images,
        pred_images_data=pred_images,
        minimum_iou=minimum_iou,
        image_data_matching_class=image_data_matching_class,
    ).T.reset_index(drop=True)
    df__detection.columns = FROZEN_DETECTION_METRIC_COLUMNS
    df__pose = get_df_keypoints_metrics(
        true_images_data=true_images,
        pred_images_data=pred_images,
        class_names=class_names,
        sigma=sigma,
    ).T.reset_index(drop=True)
    df__pose.columns = ["calc__images_support", POSE_SUPPORT_COLUMN, *POSE_VALUE_COLUMNS]

    df__metrics = pd.DataFrame(
        {
            **{col: df__detection.iloc[0][col] for col in FROZEN_DETECTION_METRIC_COLUMNS},
            **{col: df__pose.iloc[0][col] for col in POSE_METRIC_COLUMNS},
        },
        index=[0],
    )
    for primary_key in keypoints_model_primary_keys:
        df__metrics[primary_key] = idx.iloc[0][primary_key]
    df__metrics[keypoints_frozen_dataset_id__name] = frozen_id
    df__metrics["subset_id"] = subset_id
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
    class_names: Optional[List[str]] = None
    sigma: Optional[np.ndarray] = None

    def build_compute(self, ds: DataStore, catalog: Catalog) -> List[ComputeStep]:
        if self.keypoints_model_primary_keys is None:
            self.keypoints_model_primary_keys = ["keypoints_model_id"]
        check_columns_are_in_table(ds, self.input__keypoints_model, self.keypoints_model_primary_keys)
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

        dt__pred = get_datatable(ds, self.input__keypoints_prediction)
        catalog.add_datatable(
            get_pipeline_table_name(self.output__keypoints_model__metrics_on__frozen_dataset),
            Table(
                ds.get_or_create_table(
                    get_pipeline_table_name(self.output__keypoints_model__metrics_on__frozen_dataset),
                    TableStoreDB(
                        dbconn=ds.meta_dbconn,
                        name=get_pipeline_table_name(self.output__keypoints_model__metrics_on__frozen_dataset),
                        data_sql_schema=[
                            column
                            for column in dt__pred.primary_schema
                            if column.name in self.keypoints_model_primary_keys
                        ]
                        + [
                            Column(self.keypoints_frozen_dataset_id__name, String, primary_key=True),
                            Column("subset_id", String, primary_key=True),
                            *[Column(name, _float_column_type(name)) for name in FROZEN_METRIC_COLUMNS],
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
                        required_pipeline_input(self.input__keypoints_frozen_dataset__has__image_gt),
                        self.input__keypoints_model,
                        required_pipeline_input(self.input__keypoints_prediction),
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
                        class_names=self.class_names,
                        sigma=self.sigma,
                    ),
                    chunk_size=1,
                    filters=self.filters,
                ),
            ]
        )
        return build_compute(ds, catalog, pipeline)
