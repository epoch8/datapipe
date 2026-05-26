import copy
import os
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Type, Union, cast

import pandas as pd
from cv_pipeliner import ImageData
from cv_pipeliner.metrics.detection import ImageDataMatching
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
from datapipe.types import IndexDF, Labels, PipelineInput, PipelineOutput, Required
from natsort import natsorted
from sqlalchemy import JSON, Column, Float, and_, func, select, tuple_
from sqlalchemy.sql.sqltypes import Integer, String

from datapipe_ml.core.datapipe import check_columns_are_in_table, normalize_pipeline_inputs, pipeline_output_as_input
from datapipe_ml.core.image_data import (
    convert_df_with_bbox_to_df_with_image_data,
    convert_df_with_image_data_to_df_with_bbox,
)
from datapipe_ml.metrics.common import (
    CLASS_METRIC_COLUMNS,
    KNOWN_OVERALL_METRIC_COLUMNS,
    OVERALL_BASE_METRIC_COLUMNS,
    float_columns,
    macro_weighted_metric_selects,
    precision_recall_f1,
    stable_unique,
)
from datapipe_ml.tasks.detection.inference import Inference_DetectionModel
from datapipe_ml.tasks.segmentation.inference import Inference_SegmentationModel


def _required_with_keys(table_name: PipelineInput, keys: Dict[str, str]):
    required = cast(Any, Required)
    return required(table_name, keys=keys)


def count_pipeline_metrics_on_image(
    df__image__ground_truth: pd.DataFrame,
    df__subset__has__image: pd.DataFrame,
    df__pipeline_prediction: pd.DataFrame,
    primary_keys: List[str],
    minimum_iou: float,
    bbox_id__name: Optional[str],
    pipeline_model_primary_keys: List[str],
    count_by_thresholds: Optional[List[float]],
    image_data_matching_class: Type[ImageDataMatching],
    label_mapper: Optional[Dict[str, str]],
    subset_ids: Optional[List[str]] = None,
):
    key_cols = stable_unique(primary_keys + pipeline_model_primary_keys + ["subset_id"])
    out_cols = (
        key_cols
        + ["label"]
        + (["threshold"] if count_by_thresholds else [])
        + [
            "calc__images_support",
            "calc__support",
            "calc__TP",
            "calc__FP",
            "calc__FN",
            "calc__TP_extra_bbox",
            "calc__FP_extra_bbox",
            "calc__FN_extra_bbox",
        ]
    )

    if subset_ids is not None:
        df__subset__has__image = df__subset__has__image[df__subset__has__image["subset_id"].isin(subset_ids)]

    if len(df__subset__has__image) == 0 or len(df__pipeline_prediction) == 0:
        return pd.DataFrame(columns=out_cols)

    df__image__ground_truth__merge__subset = pd.merge(df__image__ground_truth, df__subset__has__image)
    df__true_images_data = convert_df_with_bbox_to_df_with_image_data(
        df__image__ground_truth__merge__subset, primary_keys + ["subset_id"], bbox_id__name
    )
    df__pipeline_prediction__merge__subset = pd.merge(df__pipeline_prediction, df__subset__has__image)
    df__pred_images_data = convert_df_with_bbox_to_df_with_image_data(
        df__pipeline_prediction__merge__subset, key_cols, bbox_id__name
    )
    df__images_data = pd.merge(
        df__true_images_data,
        df__pred_images_data,
        on=primary_keys + ["subset_id"],
        suffixes=("_true", "_pred"),
    )
    if label_mapper is not None:
        for image_data in df__images_data["image_data_true"]:
            for bbox_data in image_data.bboxes_data:
                bbox_data.label = label_mapper.get(bbox_data.label, bbox_data.label)
        for image_data in df__images_data["image_data_pred"]:
            for bbox_data in image_data.bboxes_data:
                bbox_data.label = label_mapper.get(bbox_data.label, bbox_data.label)
    thresholds = sorted(count_by_thresholds) if count_by_thresholds else [0.0]
    extra_label = "trash (extra bbox)"
    rows = []

    for i in df__images_data.index:
        base_row = {k: df__images_data.loc[i, k] for k in key_cols}

        true_img: ImageData = df__images_data.loc[i, "image_data_true"]
        pred_img_orig: ImageData = df__images_data.loc[i, "image_data_pred"]

        true_labels = [bbox.label for bbox in true_img.bboxes_data]
        pred_labels = [bbox.label for bbox in pred_img_orig.bboxes_data]
        labels = natsorted(set(true_labels).union(pred_labels))
        labels = [label for label in labels if label != extra_label]

        for threshold in thresholds:
            pred_img = copy.deepcopy(pred_img_orig)
            pred_img.bboxes_data = [
                b for b in pred_img.bboxes_data if (count_by_thresholds is None) or (b.detection_score >= threshold)
            ]

            matching = image_data_matching_class(
                true_image_data=true_img,
                pred_image_data=pred_img,
                minimum_iou=minimum_iou,
                extra_bbox_label=extra_label,
            )

            true_by_label = {label: [bbox for bbox in true_img.bboxes_data if bbox.label == label] for label in labels}

            for label in labels:
                images_support = int(len(true_by_label[label]) > 0)
                support = len(true_by_label[label])

                TP = matching.get_pipeline_TP(label=label)
                FP = matching.get_pipeline_FP(label=label)
                FN = matching.get_pipeline_FN(label=label)
                TP_extra = matching.get_pipeline_TP_extra_bbox(label=label)
                FP_extra = matching.get_pipeline_FP_extra_bbox(label=label)
                FN_extra = matching.get_pipeline_FN_extra_bbox(label=label)

                row = {
                    **base_row,
                    "label": label,
                    "threshold": threshold,
                    "calc__images_support": images_support,
                    "calc__support": support,
                    "calc__TP": TP,
                    "calc__FP": FP,
                    "calc__FN": FN,
                    "calc__TP_extra_bbox": TP_extra,
                    "calc__FP_extra_bbox": FP_extra,
                    "calc__FN_extra_bbox": FN_extra,
                }
                rows.append(row)

    if not rows:
        return pd.DataFrame(columns=out_cols)
    df__pipeline_model__metrics_on__image = pd.DataFrame(rows)
    return df__pipeline_model__metrics_on__image[out_cols]


def count_pipeline_metrics_on_subset(
    ds: DataStore,
    idx: IndexDF,
    input_dts: List[DataTable],
    run_config: Optional[RunConfig] = None,
    kwargs: Optional[Dict[str, Any]] = None,
):
    """
    Возвращает два DataFrame:
      1) По классам: (model_keys, subset_id, [threshold], label, calc__images_support, calc__support, calc__TP, calc__FP, calc__FN, calc__precision, calc__recall, calc__f1_score)
      2) Общий:      (model_keys, subset_id, [threshold], calc__images_support, calc__support, calc__accuracy, macro/weighted и *_without_pseudo_classes)
         Если задан known_class_names, то добавляются ещё 12 колонок:
         macro/weighted для known_* и known_*_without_pseudo_classes.
    """
    from sqlalchemy import cast as sql_cast

    kwargs = kwargs or {}
    pipeline_model_primary_keys: List[str] = kwargs["pipeline_model_primary_keys"]
    primary_keys: List[str] = kwargs["primary_keys"]
    pseudo_class_names: List[str] = kwargs.get("pseudo_class_names", [])
    known_class_names: Optional[List[str]] = kwargs.get("known_class_names")

    dt__per_image = input_dts[0]
    assert isinstance(dt__per_image.table_store, TableStoreDB)
    tbl = dt__per_image.table_store.data_table

    available = set(tbl.c.keys())

    col_subset = tbl.c["subset_id"]
    col_label = tbl.c["label"]
    col_thr = tbl.c["threshold"] if "threshold" in available else None

    col_img_supp = tbl.c["calc__images_support"]
    col_support = tbl.c["calc__support"]
    col_TP = tbl.c["calc__TP"]
    col_FP = tbl.c["calc__FP"]
    col_FN = tbl.c["calc__FN"]
    col_TP_ex = tbl.c["calc__TP_extra_bbox"]
    col_FP_ex = tbl.c["calc__FP_extra_bbox"]
    col_FN_ex = tbl.c["calc__FN_extra_bbox"]

    grp_keys_tbl = [tbl.c[k] for k in pipeline_model_primary_keys] + [col_subset]
    if col_thr is not None:
        grp_keys_tbl.append(col_thr)

    img_pk_cols = [tbl.c[pk] for pk in primary_keys]
    base_img_rows = (
        select(
            *[tbl.c[k] for k in pipeline_model_primary_keys],
            col_subset.label("subset_id"),
            *([col_thr.label("threshold")] if col_thr is not None else []),
            *img_pk_cols,
        )
        .select_from(tbl)
        .where(tuple_(*([tbl.c[k] for k in idx.columns])).in_(list(idx.itertuples(index=False, name=None))))
    )
    img_rows_distinct = base_img_rows.distinct().cte("img_rows_distinct")

    img_supp_cte = (
        select(
            *[img_rows_distinct.c[k] for k in pipeline_model_primary_keys],
            img_rows_distinct.c.subset_id,
            *([img_rows_distinct.c.threshold] if col_thr is not None else []),
            func.count().label("calc__images_support"),
        )
        .select_from(img_rows_distinct)
        .group_by(
            *[img_rows_distinct.c[k] for k in pipeline_model_primary_keys],
            img_rows_distinct.c.subset_id,
            *([img_rows_distinct.c.threshold] if col_thr is not None else []),
        )
        .cte("img_supp")
    )

    class_agg = (
        select(
            *[tbl.c[k] for k in pipeline_model_primary_keys],
            col_subset.label("subset_id"),
            *([col_thr.label("threshold")] if col_thr is not None else []),
            col_label.label("label"),
            func.sum(col_img_supp).label("sum_images_support"),
            func.sum(col_support).label("sum_support"),
            func.sum(col_TP).label("sum_TP"),
            func.sum(col_FP).label("sum_FP"),
            func.sum(col_FN).label("sum_FN"),
            func.sum(col_TP_ex).label("sum_TP_ex"),
            func.sum(col_FP_ex).label("sum_FP_ex"),
            func.sum(col_FN_ex).label("sum_FN_ex"),
        )
        .select_from(tbl)
        .where(tuple_(*([tbl.c[k] for k in idx.columns])).in_(list(idx.itertuples(index=False, name=None))))
        .group_by(*grp_keys_tbl, col_label)
        .cte("class_agg")
    )

    P_num = sql_cast(class_agg.c.sum_TP + class_agg.c.sum_TP_ex, Float)
    P_den = sql_cast(class_agg.c.sum_TP + class_agg.c.sum_FP + class_agg.c.sum_FP_ex, Float)
    R_den = sql_cast(class_agg.c.sum_TP + class_agg.c.sum_FN + class_agg.c.sum_FN_ex, Float)

    P_cls, R_cls, F1_cls = precision_recall_f1(P_num, P_den, R_den, sql_cast)

    df_by_cls_stmt = select(
        *[class_agg.c[k] for k in pipeline_model_primary_keys],
        class_agg.c.subset_id,
        *([class_agg.c.threshold] if col_thr is not None else []),
        class_agg.c.label,
        class_agg.c.sum_images_support.label("calc__images_support"),
        class_agg.c.sum_support.label("calc__support"),
        class_agg.c.sum_TP.label("calc__TP"),
        class_agg.c.sum_FP.label("calc__FP"),
        class_agg.c.sum_FN.label("calc__FN"),
        P_cls.label("calc__precision"),
        R_cls.label("calc__recall"),
        F1_cls.label("calc__f1_score"),
    ).select_from(class_agg)

    grp_keys_agg = [class_agg.c[k] for k in pipeline_model_primary_keys] + [class_agg.c.subset_id]
    if col_thr is not None:
        grp_keys_agg.append(class_agg.c.threshold)

    totals = (
        select(
            *[class_agg.c[k] for k in pipeline_model_primary_keys],
            class_agg.c.subset_id,
            *([class_agg.c.threshold] if col_thr is not None else []),
            func.sum(class_agg.c.sum_support).label("calc__support"),
            func.sum(class_agg.c.sum_TP).label("TPT"),
            func.sum(class_agg.c.sum_FP).label("FPT"),
            func.sum(class_agg.c.sum_FN).label("FNT"),
            func.sum(class_agg.c.sum_TP_ex).label("TPT_ex"),
            func.sum(class_agg.c.sum_FP_ex).label("FPT_ex"),
        )
        .select_from(class_agg)
        .group_by(*grp_keys_agg)
        .cte("totals")
    )

    ACC_num = sql_cast(totals.c.TPT + totals.c.TPT_ex, Float)
    ACC_den = sql_cast(totals.c.TPT + totals.c.FPT + totals.c.FNT + totals.c.TPT_ex + totals.c.FPT_ex, Float)
    ACC = ACC_num / func.nullif(ACC_den, sql_cast(0, Float))

    class_stats = class_agg

    macro_all = (
        select(
            *[class_stats.c[k] for k in pipeline_model_primary_keys],
            class_stats.c.subset_id,
            *([class_stats.c.threshold] if col_thr is not None else []),
            *macro_weighted_metric_selects(class_stats, P_cls, R_cls, F1_cls, sql_cast),
        )
        .select_from(class_stats)
        .group_by(*grp_keys_agg)
        .cte("macro_all")
    )

    if pseudo_class_names:
        class_stats_np_src = (
            select(*class_stats.c)
            .select_from(class_stats)
            .where(~class_stats.c.label.in_(tuple(pseudo_class_names)))
            .cte("class_stats_np")
        )
    else:
        class_stats_np_src = class_stats

    grp_keys_np = [class_stats_np_src.c[k] for k in pipeline_model_primary_keys] + [class_stats_np_src.c.subset_id]
    if col_thr is not None:
        grp_keys_np.append(class_stats_np_src.c.threshold)

    P_np_num = sql_cast(class_stats_np_src.c.sum_TP + class_stats_np_src.c.sum_TP_ex, Float)
    P_np_den = sql_cast(
        class_stats_np_src.c.sum_TP + class_stats_np_src.c.sum_FP + class_stats_np_src.c.sum_FP_ex, Float
    )
    R_np_den = sql_cast(
        class_stats_np_src.c.sum_TP + class_stats_np_src.c.sum_FN + class_stats_np_src.c.sum_FN_ex, Float
    )

    P_np, R_np, F1_np = precision_recall_f1(P_np_num, P_np_den, R_np_den, sql_cast)

    macro_np = (
        select(
            *[class_stats_np_src.c[k] for k in pipeline_model_primary_keys],
            class_stats_np_src.c.subset_id,
            *([class_stats_np_src.c.threshold] if col_thr is not None else []),
            *macro_weighted_metric_selects(
                class_stats_np_src, P_np, R_np, F1_np, sql_cast, suffix="_without_pseudo_classes"
            ),
        )
        .select_from(class_stats_np_src)
        .group_by(*grp_keys_np)
        .cte("macro_np")
    )

    macro_known = None
    macro_known_np = None
    if known_class_names:
        class_stats_kn_src = (
            select(*class_stats.c)
            .select_from(class_stats)
            .where(class_stats.c.label.in_(tuple(known_class_names)))
            .cte("class_stats_kn")
        )
        grp_keys_kn = [class_stats_kn_src.c[k] for k in pipeline_model_primary_keys] + [class_stats_kn_src.c.subset_id]
        if col_thr is not None:
            grp_keys_kn.append(class_stats_kn_src.c.threshold)

        P_kn_num = sql_cast(class_stats_kn_src.c.sum_TP + class_stats_kn_src.c.sum_TP_ex, Float)
        P_kn_den = sql_cast(
            class_stats_kn_src.c.sum_TP + class_stats_kn_src.c.sum_FP + class_stats_kn_src.c.sum_FP_ex, Float
        )
        R_kn_den = sql_cast(
            class_stats_kn_src.c.sum_TP + class_stats_kn_src.c.sum_FN + class_stats_kn_src.c.sum_FN_ex, Float
        )

        P_kn, R_kn, F1_kn = precision_recall_f1(P_kn_num, P_kn_den, R_kn_den, sql_cast)

        macro_known = (
            select(
                *[class_stats_kn_src.c[k] for k in pipeline_model_primary_keys],
                class_stats_kn_src.c.subset_id,
                *([class_stats_kn_src.c.threshold] if col_thr is not None else []),
                *macro_weighted_metric_selects(class_stats_kn_src, P_kn, R_kn, F1_kn, sql_cast, suffix="_known"),
            )
            .select_from(class_stats_kn_src)
            .group_by(*grp_keys_kn)
            .cte("macro_known")
        )

        if pseudo_class_names:
            class_stats_kn_np_src = (
                select(*class_stats.c)
                .select_from(class_stats)
                .where(
                    class_stats.c.label.in_(tuple(known_class_names))
                    & (~class_stats.c.label.in_(tuple(pseudo_class_names)))
                )
                .cte("class_stats_kn_np")
            )
        else:
            class_stats_kn_np_src = class_stats_kn_src

        grp_keys_kn_np = [class_stats_kn_np_src.c[k] for k in pipeline_model_primary_keys] + [
            class_stats_kn_np_src.c.subset_id
        ]
        if col_thr is not None:
            grp_keys_kn_np.append(class_stats_kn_np_src.c.threshold)

        P_kn_np_num = sql_cast(class_stats_kn_np_src.c.sum_TP + class_stats_kn_np_src.c.sum_TP_ex, Float)
        P_kn_np_den = sql_cast(
            class_stats_kn_np_src.c.sum_TP + class_stats_kn_np_src.c.sum_FP + class_stats_kn_np_src.c.sum_FP_ex, Float
        )
        R_kn_np_den = sql_cast(
            class_stats_kn_np_src.c.sum_TP + class_stats_kn_np_src.c.sum_FN + class_stats_kn_np_src.c.sum_FN_ex, Float
        )

        P_kn_np, R_kn_np, F1_kn_np = precision_recall_f1(P_kn_np_num, P_kn_np_den, R_kn_np_den, sql_cast)

        macro_known_np = (
            select(
                *[class_stats_kn_np_src.c[k] for k in pipeline_model_primary_keys],
                class_stats_kn_np_src.c.subset_id,
                *([class_stats_kn_np_src.c.threshold] if col_thr is not None else []),
                *macro_weighted_metric_selects(
                    class_stats_kn_np_src,
                    P_kn_np,
                    R_kn_np,
                    F1_kn_np,
                    sql_cast,
                    suffix="_known_without_pseudo_classes",
                ),
            )
            .select_from(class_stats_kn_np_src)
            .group_by(*grp_keys_kn_np)
            .cte("macro_known_np")
        )

    join_keys = pipeline_model_primary_keys + ["subset_id"] + (["threshold"] if col_thr is not None else [])

    def _join_on(left, right):
        return and_(*[left.c[k] == right.c[k] for k in join_keys])

    select_cols = [
        *[totals.c[k] for k in pipeline_model_primary_keys],
        totals.c.subset_id,
        *([totals.c.threshold] if col_thr is not None else []),
        img_supp_cte.c.calc__images_support,
        totals.c.calc__support,
        ACC.label("calc__accuracy"),
        macro_all.c.calc__weighted_precision,
        macro_all.c.calc__weighted_recall,
        macro_all.c.calc__weighted_f1_score,
        macro_all.c.calc__macro_precision,
        macro_all.c.calc__macro_recall,
        macro_all.c.calc__macro_f1_score,
        macro_np.c.calc__weighted_without_pseudo_classes_precision,
        macro_np.c.calc__weighted_without_pseudo_classes_recall,
        macro_np.c.calc__weighted_without_pseudo_classes_f1_score,
        macro_np.c.calc__macro_without_pseudo_classes_precision,
        macro_np.c.calc__macro_without_pseudo_classes_recall,
        macro_np.c.calc__macro_without_pseudo_classes_f1_score,
    ]

    if macro_known is not None:
        select_cols += [
            macro_known.c.calc__weighted_known_precision,
            macro_known.c.calc__weighted_known_recall,
            macro_known.c.calc__weighted_known_f1_score,
            macro_known.c.calc__macro_known_precision,
            macro_known.c.calc__macro_known_recall,
            macro_known.c.calc__macro_known_f1_score,
        ]
    if macro_known_np is not None:
        select_cols += [
            macro_known_np.c.calc__weighted_known_without_pseudo_classes_precision,
            macro_known_np.c.calc__weighted_known_without_pseudo_classes_recall,
            macro_known_np.c.calc__weighted_known_without_pseudo_classes_f1_score,
            macro_known_np.c.calc__macro_known_without_pseudo_classes_precision,
            macro_known_np.c.calc__macro_known_without_pseudo_classes_recall,
            macro_known_np.c.calc__macro_known_without_pseudo_classes_f1_score,
        ]

    overall_stmt = (
        select(*select_cols)
        .select_from(totals)
        .join(img_supp_cte, _join_on(totals, img_supp_cte))
        .join(macro_all, _join_on(totals, macro_all))
        .join(macro_np, _join_on(totals, macro_np))
    )
    if macro_known is not None:
        overall_stmt = overall_stmt.join(macro_known, _join_on(totals, macro_known), isouter=True)
    if macro_known_np is not None:
        overall_stmt = overall_stmt.join(macro_known_np, _join_on(totals, macro_known_np), isouter=True)

    with ds.meta_dbconn.con.begin() as con:
        df_by_cls = pd.read_sql(df_by_cls_stmt, con)
        df_overall = pd.read_sql(overall_stmt, con)

    ordered_by_cls = (
        pipeline_model_primary_keys
        + ["subset_id", "label"]
        + (["threshold"] if ("threshold" in df_by_cls.columns) else [])
        + CLASS_METRIC_COLUMNS
    )
    if df_by_cls.empty:
        df_by_cls = pd.DataFrame(columns=ordered_by_cls)
    else:
        df_by_cls = df_by_cls[ordered_by_cls]

    ordered_overall = (
        pipeline_model_primary_keys
        + ["subset_id"]
        + (["threshold"] if ("threshold" in df_overall.columns) else [])
        + OVERALL_BASE_METRIC_COLUMNS
    )

    if known_class_names:
        ordered_overall += KNOWN_OVERALL_METRIC_COLUMNS

    if df_overall.empty:
        df_overall = pd.DataFrame(columns=ordered_overall)
    else:
        missing = [c for c in ordered_overall if c not in df_overall.columns]
        for c in missing:
            df_overall[c] = pd.NA
        df_overall = df_overall[ordered_overall]

    return df_by_cls, df_overall


@dataclass
class CountMetrics_Subset_PipelineModel(PipelineStep):
    input__image__ground_truth: PipelineInput
    input__subset__has__image: PipelineInput
    input__pipeline_prediction: PipelineInput
    output__pipeline_model__metrics_on__image: PipelineOutput
    output__pipeline_model__metrics_by_cls_on__subset: PipelineOutput
    output__pipeline_model__metrics_on__subset: PipelineOutput
    primary_keys: List[str]
    chunk_size: int = 100
    bbox_id__name: Optional[str] = "bbox_id"
    create_table: bool = False
    labels: Optional[Labels] = None
    minimum_iou: float = 0.5
    pseudo_class_names: List[str] = field(default_factory=list)
    known_class_names: Optional[List[str]] = None
    filters: Union[LabelDict, Callable[[], LabelDict], None] = None
    pipeline_model_primary_keys: Optional[List[str]] = None
    count_by_thresholds: Optional[List[float]] = None
    subset_ids: Optional[List[str]] = None
    executor_config: Optional[ExecutorConfig] = None
    image_data_matching_class: Type[ImageDataMatching] = ImageDataMatching
    label_mapper: Optional[Dict[str, str]] = None

    def build_compute(self, ds: DataStore, catalog: Catalog) -> List[ComputeStep]:
        if self.count_by_thresholds is not None and not all(0.0 < t < 1.0 for t in self.count_by_thresholds):
            raise ValueError("count_by_thresholds must contain values between 0.0 and 1.0")
        if self.pipeline_model_primary_keys is None:
            self.pipeline_model_primary_keys = ["detection_model_id", "classification_model_id"]

        if self.filters is not None and not isinstance(self.filters, dict) and not callable(self.filters):
            raise TypeError("filters must be a dict or a zero-argument callable returning a dict")

        check_columns_are_in_table(ds, self.input__subset__has__image, ["subset_id"])
        if self.bbox_id__name is not None:
            check_columns_are_in_table(
                ds,
                self.input__image__ground_truth,
                self.primary_keys + [self.bbox_id__name, "x_min", "y_min", "x_max", "y_max", "label"],
            )
            check_columns_are_in_table(
                ds,
                self.input__pipeline_prediction,
                self.primary_keys
                + self.pipeline_model_primary_keys
                + [self.bbox_id__name, "x_min", "y_min", "x_max", "y_max", "label"],
            )
        else:
            check_columns_are_in_table(ds, self.input__image__ground_truth, self.primary_keys + ["bboxes", "labels"])
            check_columns_are_in_table(
                ds,
                self.input__pipeline_prediction,
                self.primary_keys + self.pipeline_model_primary_keys + ["bboxes", "labels"],
            )

        dt__input__image__ground_truth = ds.get_table(self.input__image__ground_truth)
        dt__input__pipeline_prediction = ds.get_table(self.input__pipeline_prediction)

        catalog.add_datatable(
            self.output__pipeline_model__metrics_on__image,
            Table(
                ds.get_or_create_table(
                    self.output__pipeline_model__metrics_on__image,
                    TableStoreDB(
                        dbconn=ds.meta_dbconn,
                        name=self.output__pipeline_model__metrics_on__image,
                        data_sql_schema=[
                            column
                            for column in dt__input__image__ground_truth.primary_schema
                            if column.name not in ["subset_id"]
                        ]
                        + [
                            column
                            for column in dt__input__pipeline_prediction.primary_schema
                            if column.name not in self.primary_keys
                        ]
                        + [
                            Column("subset_id", String, primary_key=True),
                            Column("label", String, primary_key=True),
                        ]
                        + (
                            [Column("threshold", Float, primary_key=True)]
                            if self.count_by_thresholds is not None
                            else []
                        )
                        + [
                            Column("calc__images_support", Integer),
                            Column("calc__support", Integer),
                            Column("calc__TP", Integer),
                            Column("calc__FP", Integer),
                            Column("calc__FN", Integer),
                            Column("calc__TP_extra_bbox", Integer),
                            Column("calc__FP_extra_bbox", Integer),
                            Column("calc__FN_extra_bbox", Integer),
                        ],
                        create_table=self.create_table,
                    ),
                ).table_store
            ),
        )

        catalog.add_datatable(
            self.output__pipeline_model__metrics_by_cls_on__subset,
            Table(
                ds.get_or_create_table(
                    self.output__pipeline_model__metrics_by_cls_on__subset,
                    TableStoreDB(
                        dbconn=ds.meta_dbconn,
                        name=self.output__pipeline_model__metrics_by_cls_on__subset,
                        data_sql_schema=[
                            column
                            for column in dt__input__pipeline_prediction.primary_schema
                            if column.name in self.pipeline_model_primary_keys
                        ]
                        + [
                            Column("subset_id", String, primary_key=True),
                            Column("label", String, primary_key=True),
                        ]
                        + (
                            [Column("threshold", Float, primary_key=True)]
                            if self.count_by_thresholds is not None
                            else []
                        )
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

        extra_known_columns: List[Column] = []
        if self.known_class_names:
            extra_known_columns = float_columns(KNOWN_OVERALL_METRIC_COLUMNS)

        catalog.add_datatable(
            self.output__pipeline_model__metrics_on__subset,
            Table(
                ds.get_or_create_table(
                    self.output__pipeline_model__metrics_on__subset,
                    TableStoreDB(
                        dbconn=ds.meta_dbconn,
                        name=self.output__pipeline_model__metrics_on__subset,
                        data_sql_schema=[
                            column
                            for column in dt__input__pipeline_prediction.primary_schema
                            if column.name in self.pipeline_model_primary_keys
                        ]
                        + [Column("subset_id", String, primary_key=True)]
                        + (
                            [Column("threshold", Float, primary_key=True)]
                            if self.count_by_thresholds is not None
                            else []
                        )
                        + [
                            Column("calc__images_support", Integer),
                            Column("calc__support", Integer),
                            *float_columns(OVERALL_BASE_METRIC_COLUMNS[2:]),
                        ]
                        + extra_known_columns,
                        create_table=self.create_table,
                    ),
                ).table_store
            ),
        )
        pipeline = Pipeline(
            [
                BatchTransform(
                    func=count_pipeline_metrics_on_image,
                    inputs=[
                        self.input__image__ground_truth,
                        self.input__subset__has__image,
                        self.input__pipeline_prediction,
                    ],
                    outputs=[self.output__pipeline_model__metrics_on__image],
                    transform_keys=sorted(set(self.primary_keys + self.pipeline_model_primary_keys + ["subset_id"])),
                    executor_config=self.executor_config,
                    labels=self.labels,
                    kwargs=dict(
                        primary_keys=self.primary_keys,
                        minimum_iou=self.minimum_iou,
                        bbox_id__name=self.bbox_id__name,
                        pipeline_model_primary_keys=self.pipeline_model_primary_keys,
                        count_by_thresholds=self.count_by_thresholds,
                        image_data_matching_class=self.image_data_matching_class,
                        label_mapper=self.label_mapper,
                        subset_ids=self.subset_ids,
                    ),
                    chunk_size=self.chunk_size,
                    filters=self.filters,
                ),
                DatatableBatchTransform(
                    func=count_pipeline_metrics_on_subset,
                    inputs=[pipeline_output_as_input(self.output__pipeline_model__metrics_on__image)],
                    outputs=[
                        self.output__pipeline_model__metrics_by_cls_on__subset,
                        self.output__pipeline_model__metrics_on__subset,
                    ],
                    transform_keys=self.pipeline_model_primary_keys
                    + ["subset_id"]
                    + (["threshold"] if self.count_by_thresholds else []),
                    labels=self.labels,
                    kwargs=dict(
                        pipeline_model_primary_keys=self.pipeline_model_primary_keys,
                        primary_keys=self.primary_keys,
                        pseudo_class_names=self.pseudo_class_names,
                        known_class_names=self.known_class_names,
                    ),
                    chunk_size=1,
                ),
            ]
        )
        return build_compute(ds, catalog, pipeline)


def get_classes_best_thresholds(
    df__model: pd.DataFrame,
    df__metrics_by_cls_with_thresholds: pd.DataFrame,
    model_primary_keys: List[str],
    metric__name: str,
    class_name_to_threshold__name: str,
    model_class_names__name: str,
    count_by_thresholds: List[float],
    threshold_sort_values_ascending: bool,
):
    df__metrics_by_cls_with_thresholds = df__metrics_by_cls_with_thresholds[
        df__metrics_by_cls_with_thresholds["threshold"].isin(count_by_thresholds)
    ]
    min_threshold = min(count_by_thresholds)
    model_id_to_class_name_to_threshold: Dict[Any, Dict[str, float]] = {}
    for _, df__grouped_model in df__model.groupby(model_primary_keys):
        model_id = tuple(df__grouped_model[model_primary_keys].iloc[0].values)
        if model_id not in model_id_to_class_name_to_threshold:
            model_id_to_class_name_to_threshold[model_id] = {}
        for class_name in df__grouped_model.iloc[0][model_class_names__name]:
            model_id_to_class_name_to_threshold[model_id][class_name] = min_threshold

    for _, df__grouped in df__metrics_by_cls_with_thresholds.groupby(model_primary_keys + ["label"]):
        df__grouped.sort_values(by="threshold", inplace=True, ascending=threshold_sort_values_ascending)
        label = df__grouped.iloc[0]["label"]
        model_id = tuple(df__grouped[model_primary_keys].iloc[0].values)
        if model_id not in model_id_to_class_name_to_threshold:
            continue
        # Use idxmax with NA-safe handling; skip groups with all-NA metric values
        ser_valid = df__grouped[metric__name].dropna()
        if ser_valid.empty:
            continue
        idx = ser_valid.idxmax()
        best_threshold = float(cast(Any, df__grouped.loc[idx, "threshold"]))
        model_id_to_class_name_to_threshold[model_id][label] = best_threshold
    df__model_thresholds = pd.DataFrame(
        [
            {
                **{model_id__name: model_id[i] for i, model_id__name in enumerate(model_primary_keys)},
                class_name_to_threshold__name: model_id_to_class_name_to_threshold[model_id],
            }
            for model_id in model_id_to_class_name_to_threshold
        ]
    )
    return df__model_thresholds


def filter_prediction_by_cls_best_thresholds(
    df__prediction: pd.DataFrame,
    df__model_threshold: pd.DataFrame,
    primary_keys: List[str],
    model_primary_keys: List[str],
    bbox_id__name: Optional[str],
    class_name_to_threshold__name: str,
):
    prediction_columns = df__prediction.columns
    primary_keys_total = sorted(set(primary_keys + model_primary_keys))
    df__prediction__image_data = convert_df_with_bbox_to_df_with_image_data(
        df__prediction, primary_keys=primary_keys_total, bbox_id__name=bbox_id__name
    )
    df__prediction__image_data = pd.merge(df__prediction__image_data, df__model_threshold, on=model_primary_keys)
    for image_data, class_name_to_threshold in zip(
        df__prediction__image_data["image_data"], df__prediction__image_data[class_name_to_threshold__name]
    ):
        image_data.bboxes_data = [
            bbox_data
            for bbox_data in image_data.bboxes_data
            if bbox_data.detection_score >= class_name_to_threshold[bbox_data.label]
        ]
    df__prediction = convert_df_with_image_data_to_df_with_bbox(
        df__prediction__image_data, primary_keys=primary_keys_total, bbox_id__name=bbox_id__name
    )
    return df__prediction[prediction_columns]


@dataclass
class Inference_And_FindBestThresholdsPerClasssOnSubset_DetectionModel(PipelineStep):
    input__image: PipelineInput | Sequence[PipelineInput]
    input__image__ground_truth: PipelineInput
    input__subset__has__image: PipelineInput
    input__detection_model: PipelineInput
    output__detection_prediction_raw: PipelineOutput
    output__pipeline_model__metrics_on__image__with_thresholds: PipelineOutput
    output__pipeline_model__metrics_with_thresholds: PipelineOutput
    output__pipeline_model__metrics_by_cls_and_thresholds: PipelineOutput
    output__detection_model_thresholds: PipelineOutput
    output__detection_prediction: PipelineOutput
    subset_ids: List[str]
    metric__name: str
    primary_keys: List[str]
    count_by_thresholds: List[float]
    chunk_size: int = 64
    create_table: bool = False
    labels: Optional[Labels] = None
    minimum_iou: float = 0.5
    pseudo_class_names: List[str] = field(default_factory=list)
    image__image_path__name: str = "image__image_path"
    bbox_id__name: Optional[str] = "bbox_id"
    batch_size_default: int = int(os.environ.get("DETECTION_BATCH_SIZE_DEFAULT", 64))
    inference_executor_config: Optional[ExecutorConfig] = None
    count_metrics_executor_config: Optional[ExecutorConfig] = None
    detection_model_primary_keys: Optional[List[str]] = None
    class_name_to_threshold__name: str = "class_name_to_threshold"
    threshold_sort_values_ascending: bool = True
    image_data_matching_class: Type[ImageDataMatching] = ImageDataMatching
    minimum_detection_score: float = 0.10
    modules_to_hide_when_loading_detection_model: Optional[List[str]] = None

    def build_compute(self, ds: DataStore, catalog: Catalog) -> List[ComputeStep]:
        if self.detection_model_primary_keys is None:
            self.detection_model_primary_keys = ["detection_model_id"]

        input__images = normalize_pipeline_inputs(self.input__image)
        inference_pipeline = Inference_DetectionModel(
            input__image=input__images + [self.input__image__ground_truth],
            input__detection_model=self.input__detection_model,
            output__detection_prediction=self.output__detection_prediction_raw,
            primary_keys=self.primary_keys,
            chunk_size=self.chunk_size,
            create_table=self.create_table,
            labels=self.labels,
            image__image_path__name=self.image__image_path__name,
            bbox_id__name=None,
            batch_size_default=self.batch_size_default,
            executor_config=self.inference_executor_config,
            detection_model_primary_keys=self.detection_model_primary_keys,
            prediction_threshold=self.minimum_detection_score,
            modules_to_hide_when_loading_detection_model=self.modules_to_hide_when_loading_detection_model,
        )
        raw_count_metrics_pipeline = CountMetrics_Subset_PipelineModel(
            input__image__ground_truth=self.input__image__ground_truth,
            input__subset__has__image=self.input__subset__has__image,
            input__pipeline_prediction=pipeline_output_as_input(self.output__detection_prediction_raw),
            output__pipeline_model__metrics_on__image=self.output__pipeline_model__metrics_on__image__with_thresholds,
            output__pipeline_model__metrics_by_cls_on__subset=self.output__pipeline_model__metrics_by_cls_and_thresholds,
            output__pipeline_model__metrics_on__subset=self.output__pipeline_model__metrics_with_thresholds,
            primary_keys=self.primary_keys,
            bbox_id__name=None,
            create_table=self.create_table,
            labels=self.labels,
            minimum_iou=self.minimum_iou,
            pseudo_class_names=self.pseudo_class_names,
            pipeline_model_primary_keys=self.detection_model_primary_keys,
            count_by_thresholds=self.count_by_thresholds,
            subset_ids=self.subset_ids,
            image_data_matching_class=self.image_data_matching_class,
        )
        dt__input__images = [ds.get_table(input__image) for input__image in input__images]
        dt__input_detection_model = ds.get_table(self.input__detection_model)
        catalog.add_datatable(
            self.output__detection_model_thresholds,
            Table(
                ds.get_or_create_table(
                    self.output__detection_model_thresholds,
                    TableStoreDB(
                        dbconn=ds.meta_dbconn,
                        name=self.output__detection_model_thresholds,
                        data_sql_schema=[
                            column
                            for column in dt__input_detection_model.primary_schema
                            if column.name in self.detection_model_primary_keys
                        ]
                        + [
                            Column(self.class_name_to_threshold__name, JSON),
                        ],
                        create_table=self.create_table,
                    ),
                ).table_store
            ),
        )
        if self.bbox_id__name is not None:
            catalog.add_datatable(
                self.output__detection_prediction,
                Table(
                    ds.get_or_create_table(
                        self.output__detection_prediction,
                        TableStoreDB(
                            dbconn=ds.meta_dbconn,
                            name=self.output__detection_prediction,
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
                self.output__detection_prediction,
                Table(
                    ds.get_or_create_table(
                        self.output__detection_prediction,
                        TableStoreDB(
                            dbconn=ds.meta_dbconn,
                            name=self.output__detection_prediction,
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
        pipeline = Pipeline(
            [
                BatchTransform(
                    func=get_classes_best_thresholds,
                    inputs=[
                        self.input__detection_model,
                        _required_with_keys(
                            pipeline_output_as_input(self.output__pipeline_model__metrics_by_cls_and_thresholds),
                            keys={key: key for key in self.detection_model_primary_keys},
                        ),
                    ],
                    outputs=[self.output__detection_model_thresholds],
                    transform_keys=self.detection_model_primary_keys,
                    chunk_size=self.chunk_size,
                    labels=self.labels,
                    order_by=self.detection_model_primary_keys,
                    order="desc",
                    kwargs=dict(
                        model_primary_keys=self.detection_model_primary_keys,
                        metric__name=self.metric__name,
                        class_name_to_threshold__name=self.class_name_to_threshold__name,
                        model_class_names__name="detection_model__class_names",
                        count_by_thresholds=self.count_by_thresholds,
                        threshold_sort_values_ascending=self.threshold_sort_values_ascending,
                    ),
                    executor_config=self.count_metrics_executor_config,
                ),
                BatchTransform(
                    func=filter_prediction_by_cls_best_thresholds,
                    inputs=[
                        pipeline_output_as_input(self.output__detection_prediction_raw),
                        pipeline_output_as_input(self.output__detection_model_thresholds),
                    ],
                    outputs=[self.output__detection_prediction],
                    transform_keys=stable_unique(self.primary_keys + self.detection_model_primary_keys),
                    chunk_size=self.chunk_size,
                    labels=self.labels,
                    order_by=self.detection_model_primary_keys,
                    order="desc",
                    kwargs=dict(
                        primary_keys=self.primary_keys,
                        model_primary_keys=self.detection_model_primary_keys,
                        bbox_id__name=self.bbox_id__name,
                        class_name_to_threshold__name=self.class_name_to_threshold__name,
                    ),
                    executor_config=self.count_metrics_executor_config,
                ),
            ]
        )
        return (
            inference_pipeline.build_compute(ds, catalog)
            + raw_count_metrics_pipeline.build_compute(ds, catalog)
            + build_compute(ds, catalog, pipeline)
        )


@dataclass
class Inference_And_FindBestThresholdsPerClasssOnSubset_SegmentationModel(PipelineStep):
    input__image: PipelineInput | Sequence[PipelineInput]
    input__image__ground_truth: PipelineInput
    input__subset__has__image: PipelineInput
    input__segmentation_model: PipelineInput
    output__segmentation_prediction_raw: PipelineOutput
    output__pipeline_model__metrics_on__image__with_thresholds: PipelineOutput
    output__pipeline_model__metrics_with_thresholds: PipelineOutput
    output__pipeline_model__metrics_by_cls_and_thresholds: PipelineOutput
    output__segmentation_model_thresholds: PipelineOutput
    output__segmentation_prediction: PipelineOutput
    subset_ids: List[str]
    metric__name: str
    primary_keys: List[str]
    count_by_thresholds: List[float]
    chunk_size: int = 64
    create_table: bool = False
    labels: Optional[Labels] = None
    minimum_iou: float = 0.5
    pseudo_class_names: List[str] = field(default_factory=list)
    image__image_path__name: str = "image__image_path"
    bbox_id__name: Optional[str] = "bbox_id"
    batch_size_default: int = int(os.environ.get("DETECTION_BATCH_SIZE_DEFAULT", 64))
    inference_executor_config: Optional[ExecutorConfig] = None
    count_metrics_executor_config: Optional[ExecutorConfig] = None
    segmentation_model_primary_keys: Optional[List[str]] = None
    class_name_to_threshold__name: str = "class_name_to_threshold"
    threshold_sort_values_ascending: bool = True
    image_data_matching_class: Type[ImageDataMatching] = ImageDataMatching
    minimum_detection_score: float = 0.1

    def build_compute(self, ds: DataStore, catalog: Catalog) -> List[ComputeStep]:
        if self.segmentation_model_primary_keys is None:
            self.segmentation_model_primary_keys = ["segmentation_model_id"]

        input__images = normalize_pipeline_inputs(self.input__image)
        inference_pipeline = Inference_SegmentationModel(
            input__image=input__images + [self.input__image__ground_truth],
            input__segmentation_model=self.input__segmentation_model,
            output__segmentation_prediction=self.output__segmentation_prediction_raw,
            primary_keys=self.primary_keys,
            chunk_size=self.chunk_size,
            create_table=self.create_table,
            labels=self.labels,
            image__image_path__name=self.image__image_path__name,
            bbox_id__name=None,
            batch_size_default=self.batch_size_default,
            executor_config=self.inference_executor_config,
            segmentation_model_primary_keys=self.segmentation_model_primary_keys,
            prediction_threshold=self.minimum_detection_score,
        )
        raw_count_metrics_pipeline = CountMetrics_Subset_PipelineModel(
            input__image__ground_truth=self.input__image__ground_truth,
            input__subset__has__image=self.input__subset__has__image,
            input__pipeline_prediction=pipeline_output_as_input(self.output__segmentation_prediction_raw),
            output__pipeline_model__metrics_on__image=self.output__pipeline_model__metrics_on__image__with_thresholds,
            output__pipeline_model__metrics_by_cls_on__subset=(
                self.output__pipeline_model__metrics_by_cls_and_thresholds
            ),
            output__pipeline_model__metrics_on__subset=self.output__pipeline_model__metrics_with_thresholds,
            primary_keys=self.primary_keys,
            bbox_id__name=None,
            create_table=self.create_table,
            labels=self.labels,
            minimum_iou=self.minimum_iou,
            pseudo_class_names=self.pseudo_class_names,
            pipeline_model_primary_keys=self.segmentation_model_primary_keys,
            count_by_thresholds=self.count_by_thresholds,
            subset_ids=self.subset_ids,
            image_data_matching_class=self.image_data_matching_class,
        )
        dt__input__images = [ds.get_table(input__image) for input__image in input__images]
        dt__input_segmentation_model = ds.get_table(self.input__segmentation_model)
        catalog.add_datatable(
            self.output__segmentation_model_thresholds,
            Table(
                ds.get_or_create_table(
                    self.output__segmentation_model_thresholds,
                    TableStoreDB(
                        dbconn=ds.meta_dbconn,
                        name=self.output__segmentation_model_thresholds,
                        data_sql_schema=[
                            column
                            for column in dt__input_segmentation_model.primary_schema
                            if column.name in self.segmentation_model_primary_keys
                        ]
                        + [
                            Column(self.class_name_to_threshold__name, JSON),
                        ],
                        create_table=self.create_table,
                    ),
                ).table_store
            ),
        )
        if self.bbox_id__name is not None:
            catalog.add_datatable(
                self.output__segmentation_prediction,
                Table(
                    ds.get_or_create_table(
                        self.output__segmentation_prediction,
                        TableStoreDB(
                            dbconn=ds.meta_dbconn,
                            name=self.output__segmentation_prediction,
                            data_sql_schema=[
                                column
                                for column in dt__input__images[0].primary_schema
                                if column.name in self.primary_keys
                            ]
                            + [
                                column
                                for column in dt__input_segmentation_model.primary_schema
                                if column.name not in self.primary_keys
                            ]
                            + [
                                Column(self.bbox_id__name, String, primary_key=True),
                                Column("x_min", Integer),
                                Column("y_min", Integer),
                                Column("x_max", Integer),
                                Column("y_max", Integer),
                                Column("label", String),
                                Column("mask", JSON),
                                Column("prediction__detection_score", Float),
                            ],
                            create_table=self.create_table,
                        ),
                    ).table_store
                ),
            )
        else:
            catalog.add_datatable(
                self.output__segmentation_prediction,
                Table(
                    ds.get_or_create_table(
                        self.output__segmentation_prediction,
                        TableStoreDB(
                            dbconn=ds.meta_dbconn,
                            name=self.output__segmentation_prediction,
                            data_sql_schema=[
                                column
                                for column in dt__input__images[0].primary_schema
                                if column.name in self.primary_keys
                            ]
                            + [
                                column
                                for column in dt__input_segmentation_model.primary_schema
                                if column.name not in self.primary_keys
                            ]
                            + [
                                Column("bboxes", JSON),
                                Column("labels", JSON),
                                Column("masks", JSON),
                                Column("prediction__detection_scores", JSON),
                            ],
                            create_table=self.create_table,
                        ),
                    ).table_store
                ),
            )
        pipeline = Pipeline(
            [
                BatchTransform(
                    func=get_classes_best_thresholds,
                    inputs=[
                        self.input__segmentation_model,
                        _required_with_keys(
                            pipeline_output_as_input(self.output__pipeline_model__metrics_by_cls_and_thresholds),
                            keys={key: key for key in self.segmentation_model_primary_keys},
                        ),
                    ],
                    outputs=[self.output__segmentation_model_thresholds],
                    transform_keys=self.segmentation_model_primary_keys,
                    chunk_size=self.chunk_size,
                    labels=self.labels,
                    order_by=self.segmentation_model_primary_keys,
                    order="desc",
                    kwargs=dict(
                        model_primary_keys=self.segmentation_model_primary_keys,
                        metric__name=self.metric__name,
                        class_name_to_threshold__name=self.class_name_to_threshold__name,
                        model_class_names__name="segmentation_model__class_names",
                        count_by_thresholds=self.count_by_thresholds,
                        threshold_sort_values_ascending=self.threshold_sort_values_ascending,
                    ),
                    executor_config=self.count_metrics_executor_config,
                ),
                BatchTransform(
                    func=filter_prediction_by_cls_best_thresholds,
                    inputs=[
                        pipeline_output_as_input(self.output__segmentation_prediction_raw),
                        pipeline_output_as_input(self.output__segmentation_model_thresholds),
                    ],
                    outputs=[self.output__segmentation_prediction],
                    transform_keys=stable_unique(self.primary_keys + self.segmentation_model_primary_keys),
                    chunk_size=self.chunk_size,
                    labels=self.labels,
                    order_by=self.segmentation_model_primary_keys,
                    order="desc",
                    kwargs=dict(
                        primary_keys=self.primary_keys,
                        model_primary_keys=self.segmentation_model_primary_keys,
                        bbox_id__name=self.bbox_id__name,
                        class_name_to_threshold__name=self.class_name_to_threshold__name,
                    ),
                    executor_config=self.count_metrics_executor_config,
                ),
            ]
        )
        return (
            inference_pipeline.build_compute(ds, catalog)
            + raw_count_metrics_pipeline.build_compute(ds, catalog)
            + build_compute(ds, catalog, pipeline)
        )
