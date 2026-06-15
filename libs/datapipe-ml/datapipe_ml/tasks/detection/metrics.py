from collections.abc import Callable
from dataclasses import dataclass
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
from datapipe.datatable import DataStore, DataTable
from datapipe.executor import ExecutorConfig
from datapipe.run_config import LabelDict, RunConfig
from datapipe.step.batch_transform import BatchTransform, DatatableBatchTransform
from datapipe.store.database import TableStoreDB
from datapipe.types import PipelineInput, PipelineOutput, IndexDF, Labels
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
from datapipe_ml.metrics.common import stable_unique


def count_detection_metrics_on_image(
    df__image__ground_truth: pd.DataFrame,
    df__subset__has__image: pd.DataFrame,
    df__detection_prediction: pd.DataFrame,
    primary_keys: List[str],
    minimum_iou: float,
    bbox_id__name: Optional[str],
    detection_model_primary_keys: List[str],
    image_data_matching_class: Type[ImageDataMatching],
):
    if len(df__subset__has__image) == 0 or len(df__detection_prediction) == 0:
        return pd.DataFrame(
            columns=detection_model_primary_keys
            + [
                "subset_id",
                "calc__TP",
                "calc__FP",
                "calc__FN",
                "calc__iou_mean",
                "calc__accuracy",
                "calc__precision",
                "calc__recall",
                "calc__f1_score",
            ]
        )
    key_cols = stable_unique(primary_keys + detection_model_primary_keys + ["subset_id"])
    df__image__ground_truth__merge__subset = pd.merge(df__image__ground_truth, df__subset__has__image)
    df__true_images_data = convert_df_with_bbox_to_df_with_image_data(
        df__image__ground_truth__merge__subset, primary_keys + ["subset_id"], bbox_id__name
    )
    df__detection_prediction__merge__subset = pd.merge(df__detection_prediction, df__subset__has__image)
    df__pred_images_data = convert_df_with_bbox_to_df_with_image_data(
        df__detection_prediction__merge__subset, key_cols, bbox_id__name
    )
    df__images_data = pd.merge(
        df__true_images_data, df__pred_images_data, on=primary_keys + ["subset_id"], suffixes=("_true", "_pred")
    )
    detection_metrics_on__image = []
    for idx in df__images_data.index:
        image_data_true = df__images_data.loc[idx, "image_data_true"]
        image_data_pred = df__images_data.loc[idx, "image_data_pred"]
        df__detection_metrics_on__idx = get_df_detection_metrics(
            true_images_data=[image_data_true],
            pred_images_data=[image_data_pred],
            minimum_iou=minimum_iou,
            image_data_matching_class=image_data_matching_class,
        ).T.reset_index(drop=True)
        df__detection_metrics_on__idx.columns = [
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
        for primary_key in key_cols:
            df__detection_metrics_on__idx[primary_key] = df__images_data.loc[idx, primary_key]
        detection_metrics_on__image.append(df__detection_metrics_on__idx)
    columns = stable_unique(primary_keys + detection_model_primary_keys + ["subset_id"])
    columns.extend(["calc__support", "calc__TP", "calc__FP", "calc__FN", "calc__iou_mean"])
    if len(detection_metrics_on__image) == 0:
        df__detection_metrics_on__image = pd.DataFrame(
            columns=columns
            + [
                "subset_id",
                "calc__support",
                "calc__TP",
                "calc__FP",
                "calc__FN",
                "calc__iou_mean",
            ]
        )
    else:
        df__detection_metrics_on__image = pd.concat(detection_metrics_on__image)
    return df__detection_metrics_on__image[columns]


def count_detection_metrics_on_subset(
    ds: DataStore,
    idx: IndexDF,
    input_dts: List[DataTable],
    run_config: Optional[RunConfig] = None,
    kwargs: Optional[Dict[str, Any]] = None,
):
    """
    Same logic as in get_df_detection_metrics
    """

    kwargs = kwargs or {}
    detection_model_primary_keys: List[str] = kwargs["detection_model_primary_keys"]

    dt__detection_metrics_on_image = input_dts[0]
    assert isinstance(dt__detection_metrics_on_image.table_store, TableStoreDB)
    tbl = dt__detection_metrics_on_image.table_store.data_table

    col_subset_id = tbl.c["subset_id"]
    col_support = tbl.c["calc__support"]
    col_TP = tbl.c["calc__TP"]
    col_FP = tbl.c["calc__FP"]
    col_FN = tbl.c["calc__FN"]
    col_iou_mean = tbl.c["calc__iou_mean"]

    group_cols = [tbl.c[k] for k in detection_model_primary_keys] + [col_subset_id]

    # image count = number of per-image rows in the group
    img_count = func.count().label("calc__images_support")

    sum_support = func.sum(col_support).label("calc__support")
    sum_TP = func.sum(col_TP).label("calc__TP")
    sum_FP = func.sum(col_FP).label("calc__FP")
    sum_FN = func.sum(col_FN).label("calc__FN")

    # support-weighted mean IoU
    weighted_iou_num = func.sum(sql_cast(col_iou_mean, Float) * sql_cast(col_support, Float))
    weighted_iou_den = func.nullif(sql_cast(func.sum(col_support), Float), sql_cast(0, Float))
    agg_iou_mean = (weighted_iou_num / weighted_iou_den).label("calc__iou_mean")

    # metrics with float division
    prec = sql_cast(sum_TP, Float) / func.nullif(sql_cast(sum_TP + sum_FP, Float), sql_cast(0, Float))
    rec = sql_cast(sum_TP, Float) / func.nullif(sql_cast(sum_TP + sum_FN, Float), sql_cast(0, Float))
    acc = sql_cast(sum_TP, Float) / func.nullif(sql_cast(sum_TP + sum_FP + sum_FN, Float), sql_cast(0, Float))
    f1 = (sql_cast(2.0, Float) * prec * rec) / func.nullif(prec + rec, sql_cast(0, Float))

    stmt = (
        select(
            *[tbl.c[k] for k in detection_model_primary_keys],
            col_subset_id.label("subset_id"),
            img_count,
            sum_support,
            sum_TP,
            sum_FP,
            sum_FN,
            agg_iou_mean,
            acc.label("calc__accuracy"),
            prec.label("calc__precision"),
            rec.label("calc__recall"),
            f1.label("calc__f1_score"),
        )
        .select_from(tbl)
        .where(tuple_(*([tbl.c[k] for k in idx.columns])).in_(list(idx.itertuples(index=False, name=None))))
        .group_by(*group_cols)
    )

    with ds.meta_dbconn.con.begin() as con:
        df = pd.read_sql(stmt, con)

    ordered_cols = detection_model_primary_keys + [
        "subset_id",
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

    if df.empty:
        df = pd.DataFrame(columns=ordered_cols)

    return df[ordered_cols]


@dataclass
class CountMetrics_Subset_DetectionModel(PipelineStep):
    input__image__ground_truth: PipelineInput
    input__subset__has__image: PipelineInput
    input__detection_prediction: PipelineInput
    output__detection_model__metrics__on__image: PipelineOutput
    output__detection_model__metrics__on__subset: PipelineOutput
    primary_keys: List[str]
    bbox_id__name: Optional[str] = "bbox_id"
    create_table: bool = False
    labels: Optional[Labels] = None
    minimum_iou: float = 0.5
    chunk_size: int = 100
    filters: Union[LabelDict, Callable[[], LabelDict], None] = None
    detection_model_primary_keys: Optional[List[str]] = None
    executor_config: Optional[ExecutorConfig] = None
    image_data_matching_class: Type[ImageDataMatching] = ImageDataMatching

    def build_compute(self, ds: DataStore, catalog: Catalog) -> List[ComputeStep]:
        if self.detection_model_primary_keys is None:
            self.detection_model_primary_keys = ["detection_model_id"]
        check_columns_are_in_table(ds, self.input__subset__has__image, ["subset_id"])
        if self.bbox_id__name is not None:
            check_columns_are_in_table(
                ds,
                self.input__image__ground_truth,
                self.primary_keys + [self.bbox_id__name, "x_min", "y_min", "x_max", "y_max"],
            )
            check_columns_are_in_table(
                ds,
                self.input__detection_prediction,
                self.primary_keys
                + self.detection_model_primary_keys
                + [self.bbox_id__name, "x_min", "y_min", "x_max", "y_max"],
            )
        else:
            check_columns_are_in_table(
                ds,
                self.input__image__ground_truth,
                self.primary_keys + ["bboxes"],
            )
            check_columns_are_in_table(
                ds,
                self.input__detection_prediction,
                self.primary_keys + self.detection_model_primary_keys + ["bboxes"],
            )
        # ---
        dt__input__image__ground_truth = get_datatable(ds, self.input__image__ground_truth)
        dt__input__detection_prediction = get_datatable(ds, self.input__detection_prediction)
        catalog.add_datatable(
            get_pipeline_table_name(self.output__detection_model__metrics__on__image),
            Table(
                ds.get_or_create_table(
                    get_pipeline_table_name(self.output__detection_model__metrics__on__image),
                    TableStoreDB(
                        dbconn=ds.meta_dbconn,
                        name=get_pipeline_table_name(self.output__detection_model__metrics__on__image),
                        data_sql_schema=[
                            column
                            for column in dt__input__image__ground_truth.primary_schema
                            if column.name not in ["subset_id"]
                        ]
                        + [
                            column
                            for column in dt__input__detection_prediction.primary_schema
                            if column.name not in self.primary_keys
                        ]
                        + [
                            Column("subset_id", String, primary_key=True),
                            Column("calc__support", Integer),
                            Column("calc__TP", Integer),
                            Column("calc__FP", Integer),
                            Column("calc__FN", Integer),
                            Column("calc__iou_mean", Float),
                        ],
                        create_table=self.create_table,
                    ),
                ).table_store
            ),
        )
        catalog.add_datatable(
            get_pipeline_table_name(self.output__detection_model__metrics__on__subset),
            Table(
                ds.get_or_create_table(
                    get_pipeline_table_name(self.output__detection_model__metrics__on__subset),
                    TableStoreDB(
                        dbconn=ds.meta_dbconn,
                        name=get_pipeline_table_name(self.output__detection_model__metrics__on__subset),
                        data_sql_schema=[
                            column
                            for column in dt__input__detection_prediction.primary_schema
                            if column.name in self.detection_model_primary_keys
                        ]
                        + [
                            Column("subset_id", String, primary_key=True),
                            Column("calc__images_support", Integer),
                            Column("calc__support", Integer),
                            Column("calc__TP", Integer),
                            Column("calc__FP", Integer),
                            Column("calc__FN", Integer),
                            Column("calc__iou_mean", Float),
                            Column("calc__accuracy", Float),
                            Column("calc__precision", Float),
                            Column("calc__recall", Float),
                            Column("calc__f1_score", Float),
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
                    func=count_detection_metrics_on_image,
                    inputs=[
                        self.input__image__ground_truth,
                        self.input__subset__has__image,
                        self.input__detection_prediction,
                    ],
                    outputs=[self.output__detection_model__metrics__on__image],
                    transform_keys=stable_unique(self.primary_keys + self.detection_model_primary_keys),
                    executor_config=self.executor_config,
                    labels=self.labels,
                    kwargs=dict(
                        primary_keys=self.primary_keys,
                        minimum_iou=self.minimum_iou,
                        bbox_id__name=self.bbox_id__name,
                        detection_model_primary_keys=self.detection_model_primary_keys,
                        image_data_matching_class=self.image_data_matching_class,
                    ),
                    chunk_size=self.chunk_size,
                    filters=self.filters,
                ),
                DatatableBatchTransform(
                    func=count_detection_metrics_on_subset,
                    inputs=[pipeline_output_as_input(self.output__detection_model__metrics__on__image)],
                    outputs=[self.output__detection_model__metrics__on__subset],
                    transform_keys=self.detection_model_primary_keys + ["subset_id"],
                    labels=self.labels,
                    kwargs=dict(
                        primary_keys=self.primary_keys,
                        detection_model_primary_keys=self.detection_model_primary_keys,
                    ),
                    chunk_size=1,
                ),
            ]
        )
        return build_compute(ds, catalog, pipeline)
