from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Union

import pandas as pd
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
from sqlalchemy import Column, Float, and_, func, select
from sqlalchemy.sql.sqltypes import Integer, String

from datapipe_ml.core.datapipe import check_columns_are_in_table, get_datatable, get_pipeline_table_name, pipeline_output_as_input
from datapipe_ml.metrics.common import (
    CLASS_METRIC_COLUMNS,
    KNOWN_OVERALL_METRIC_COLUMNS,
    OVERALL_BASE_METRIC_COLUMNS,
    float_columns,
    idx_in_table_clause,
    macro_weighted_metric_selects,
    precision_recall_f1,
    stable_unique,
)
from datapipe_ml.metrics.inputs import (
    build_ground_truth_batch_inputs,
    get_ground_truth_datatables,
    merged_ground_truth_primary_schema,
    model_primary_key_columns,
    model_primary_keys_in_table,
    primary_ground_truth_input,
    wrap_ground_truth_inputs,
)


def count_classification_metrics_on_image(
    df__image__ground_truth: pd.DataFrame,
    df__subset__has__image: pd.DataFrame,
    df__classification_prediction: pd.DataFrame,
    primary_keys: List[str],
    classification_model_primary_keys: List[str],
    label_mapper: Optional[Dict[str, str]] = None,
):
    df_pred = df__classification_prediction[df__classification_prediction["prediction__top_n"] == 1]
    key_cols = stable_unique(primary_keys + classification_model_primary_keys + ["subset_id"])
    out_cols = key_cols + [
        "label",
        "calc__images_support",
        "calc__support",
        "calc__TP",
        "calc__FP",
        "calc__FN",
    ]

    if len(df__subset__has__image) == 0 or len(df_pred) == 0:
        return pd.DataFrame(columns=out_cols)

    df_gt = pd.merge(df__image__ground_truth, df__subset__has__image)
    df_pred = pd.merge(df_pred, df__subset__has__image)
    df = pd.merge(df_gt, df_pred, on=primary_keys + ["subset_id"], suffixes=("_gt", "_pred"))
    if label_mapper is not None:
        mapper = label_mapper
        df["label_gt"] = df["label_gt"].apply(lambda x: mapper.get(x, x))
        df["label_pred"] = df["label_pred"].apply(lambda x: mapper.get(x, x))

    rows: List[Dict] = []
    for _, r in df.iterrows():
        base = {k: r[k] for k in key_cols}
        true_label = r["label_gt"]
        pred_label = r["label_pred"]

        rows.append(
            {
                **base,
                "label": true_label,
                "calc__images_support": 1,
                "calc__support": 1,
                "calc__TP": 1 if pred_label == true_label else 0,
                "calc__FP": 0,
                "calc__FN": 0 if pred_label == true_label else 1,
            }
        )
        if pred_label != true_label:
            rows.append(
                {
                    **base,
                    "label": pred_label,
                    "calc__images_support": 0,
                    "calc__support": 0,
                    "calc__TP": 0,
                    "calc__FP": 1,
                    "calc__FN": 0,
                }
            )

    return pd.DataFrame(rows, columns=out_cols) if rows else pd.DataFrame(columns=out_cols)


def count_classification_metrics_on_subset(
    ds: DataStore,
    idx: IndexDF,
    input_dts: List[DataTable],
    run_config: Optional[RunConfig] = None,
    kwargs: Optional[Dict] = None,
):
    """
    Aggregates per-image rows into:
      1) per-class: (model_keys, subset_id, label, ... P/R/F1)
      2) overall:   (model_keys, subset_id, images_support, support, accuracy, macro/weighted and *_without_pseudo_classes)
         When known_class_names is set, macro/weighted metrics are also computed
         for known_* and known_*_without_pseudo_classes (12 columns).
    """
    from sqlalchemy import cast as sql_cast

    kwargs = kwargs or {}
    classification_model_primary_keys: List[str] = kwargs["classification_model_primary_keys"]
    pseudo_class_names: List[str] = kwargs.get("pseudo_class_names", [])
    known_class_names: Optional[List[str]] = kwargs.get("known_class_names")

    dt_per_image = input_dts[0]
    assert isinstance(dt_per_image.table_store, TableStoreDB)
    tbl = dt_per_image.table_store.data_table

    col_subset = tbl.c["subset_id"]
    col_label = tbl.c["label"]

    col_img_supp = tbl.c["calc__images_support"]
    col_support = tbl.c["calc__support"]
    col_TP = tbl.c["calc__TP"]
    col_FP = tbl.c["calc__FP"]
    col_FN = tbl.c["calc__FN"]

    grp_keys_tbl = [tbl.c[k] for k in classification_model_primary_keys] + [col_subset]

    class_agg = (
        select(
            *[tbl.c[k] for k in classification_model_primary_keys],
            col_subset.label("subset_id"),
            col_label.label("label"),
            func.sum(col_img_supp).label("sum_images_support"),
            func.sum(col_support).label("sum_support"),
            func.sum(col_TP).label("sum_TP"),
            func.sum(col_FP).label("sum_FP"),
            func.sum(col_FN).label("sum_FN"),
        )
        .select_from(tbl)
        .where(idx_in_table_clause(tbl, idx))
        .group_by(*grp_keys_tbl, col_label)
        .cte("class_agg")
    )

    P_cls, R_cls, F1_cls = precision_recall_f1(
        sql_cast(class_agg.c.sum_TP, Float),
        sql_cast(class_agg.c.sum_TP + class_agg.c.sum_FP, Float),
        sql_cast(class_agg.c.sum_TP + class_agg.c.sum_FN, Float),
        sql_cast,
    )

    df_by_cls_stmt = select(
        *[class_agg.c[k] for k in classification_model_primary_keys],
        class_agg.c.subset_id,
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

    grp_keys_agg = [class_agg.c[k] for k in classification_model_primary_keys] + [class_agg.c.subset_id]

    totals = (
        select(
            *[class_agg.c[k] for k in classification_model_primary_keys],
            class_agg.c.subset_id,
            func.sum(class_agg.c.sum_support).label("calc__support"),
            func.sum(class_agg.c.sum_TP).label("TPT"),
            func.sum(class_agg.c.sum_FP).label("FPT"),
            func.sum(class_agg.c.sum_FN).label("FNT"),
        )
        .select_from(class_agg)
        .group_by(*grp_keys_agg)
        .cte("totals")
    )

    img_supp_cte = (
        select(
            *[class_agg.c[k] for k in classification_model_primary_keys],
            class_agg.c.subset_id,
            func.sum(class_agg.c.sum_images_support).label("calc__images_support"),
        )
        .select_from(class_agg)
        .group_by(*grp_keys_agg)
        .cte("img_supp")
    )

    ACC = sql_cast(totals.c.TPT, Float) / func.nullif(
        sql_cast(totals.c.TPT + totals.c.FPT + totals.c.FNT, Float), sql_cast(0, Float)
    )

    class_stats = class_agg

    macro_all = (
        select(
            *[class_stats.c[k] for k in classification_model_primary_keys],
            class_stats.c.subset_id,
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

    grp_keys_np = [class_stats_np_src.c[k] for k in classification_model_primary_keys] + [
        class_stats_np_src.c.subset_id
    ]

    P_np, R_np, F1_np = precision_recall_f1(
        sql_cast(class_stats_np_src.c.sum_TP, Float),
        sql_cast(class_stats_np_src.c.sum_TP + class_stats_np_src.c.sum_FP, Float),
        sql_cast(class_stats_np_src.c.sum_TP + class_stats_np_src.c.sum_FN, Float),
        sql_cast,
    )

    macro_np = (
        select(
            *[class_stats_np_src.c[k] for k in classification_model_primary_keys],
            class_stats_np_src.c.subset_id,
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
        grp_keys_kn = [class_stats_kn_src.c[k] for k in classification_model_primary_keys] + [
            class_stats_kn_src.c.subset_id
        ]

        P_kn, R_kn, F1_kn = precision_recall_f1(
            sql_cast(class_stats_kn_src.c.sum_TP, Float),
            sql_cast(class_stats_kn_src.c.sum_TP + class_stats_kn_src.c.sum_FP, Float),
            sql_cast(class_stats_kn_src.c.sum_TP + class_stats_kn_src.c.sum_FN, Float),
            sql_cast,
        )

        macro_known = (
            select(
                *[class_stats_kn_src.c[k] for k in classification_model_primary_keys],
                class_stats_kn_src.c.subset_id,
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

        grp_keys_kn_np = [class_stats_kn_np_src.c[k] for k in classification_model_primary_keys] + [
            class_stats_kn_np_src.c.subset_id
        ]

        P_kn_np, R_kn_np, F1_kn_np = precision_recall_f1(
            sql_cast(class_stats_kn_np_src.c.sum_TP, Float),
            sql_cast(class_stats_kn_np_src.c.sum_TP + class_stats_kn_np_src.c.sum_FP, Float),
            sql_cast(class_stats_kn_np_src.c.sum_TP + class_stats_kn_np_src.c.sum_FN, Float),
            sql_cast,
        )

        macro_known_np = (
            select(
                *[class_stats_kn_np_src.c[k] for k in classification_model_primary_keys],
                class_stats_kn_np_src.c.subset_id,
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

    join_keys = classification_model_primary_keys + ["subset_id"]

    def _join_on(left, right):
        return and_(*[left.c[k] == right.c[k] for k in join_keys])

    select_cols = [
        *[totals.c[k] for k in classification_model_primary_keys],
        totals.c.subset_id,
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

    ordered_by_cls = classification_model_primary_keys + ["subset_id", "label"] + CLASS_METRIC_COLUMNS
    if df_by_cls.empty:
        df_by_cls = pd.DataFrame(columns=ordered_by_cls)
    else:
        df_by_cls = df_by_cls[ordered_by_cls]

    ordered_overall = classification_model_primary_keys + ["subset_id"] + OVERALL_BASE_METRIC_COLUMNS
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
class CountMetrics_Subset_ClassificationModel(PipelineStep):
    input__image__ground_truth: PipelineInput | Sequence[PipelineInput]
    input__subset__has__image: PipelineInput
    input__classification_prediction: PipelineInput
    output__classification_model__metrics__on__image: PipelineOutput
    output__classification_model__metrics_by_cls_on__subset: PipelineOutput
    output__classification_model__metrics_on__subset: PipelineOutput
    primary_keys: List[str]
    chunk_size: int = 100
    create_table: bool = False
    labels: Optional[Labels] = None
    pseudo_class_names: List[str] = field(default_factory=list)
    known_class_names: Optional[List[str]] = None
    filters: Union[LabelDict, Callable[[], LabelDict], None] = None
    classification_model_primary_keys: Optional[List[str]] = None
    executor_config: Optional[ExecutorConfig] = None
    label_mapper: Optional[Dict[str, str]] = None

    def build_compute(self, ds: DataStore, catalog: Catalog) -> List[ComputeStep]:
        if self.classification_model_primary_keys is None:
            self.classification_model_primary_keys = ["classification_model_id"]

        primary_gt = primary_ground_truth_input(self.input__image__ground_truth)
        prediction_model_keys = model_primary_keys_in_table(
            ds, self.input__classification_prediction, self.classification_model_primary_keys
        )
        check_columns_are_in_table(ds, self.input__image__ground_truth, self.primary_keys)
        check_columns_are_in_table(ds, primary_gt, self.primary_keys + ["label"])
        check_columns_are_in_table(ds, self.input__subset__has__image, ["subset_id"])
        check_columns_are_in_table(
            ds,
            self.input__classification_prediction,
            self.primary_keys + prediction_model_keys + ["label", "prediction__top_n"],
        )

        dt__image__ground_truth_tables = get_ground_truth_datatables(ds, self.input__image__ground_truth)
        dt__classification_prediction = get_datatable(ds, self.input__classification_prediction)
        model_key_columns = model_primary_key_columns(
            ds,
            self.classification_model_primary_keys,
            prediction=self.input__classification_prediction,
            ground_truth=self.input__image__ground_truth,
        )

        catalog.add_datatable(
            get_pipeline_table_name(self.output__classification_model__metrics__on__image),
            Table(
                ds.get_or_create_table(
                    get_pipeline_table_name(self.output__classification_model__metrics__on__image),
                    TableStoreDB(
                        dbconn=ds.meta_dbconn,
                        name=get_pipeline_table_name(self.output__classification_model__metrics__on__image),
                        data_sql_schema=merged_ground_truth_primary_schema(dt__image__ground_truth_tables)
                        + [
                            column
                            for column in dt__classification_prediction.primary_schema
                            if column.name not in self.primary_keys + ["prediction__top_n"]
                        ]
                        + [
                            Column("subset_id", String, primary_key=True),
                            Column("label", String, primary_key=True),
                            Column("calc__images_support", Integer),
                            Column("calc__support", Integer),
                            Column("calc__TP", Integer),
                            Column("calc__FP", Integer),
                            Column("calc__FN", Integer),
                        ],
                        create_table=self.create_table,
                    ),
                ).table_store
            ),
        )

        catalog.add_datatable(
            get_pipeline_table_name(self.output__classification_model__metrics_by_cls_on__subset),
            Table(
                ds.get_or_create_table(
                    get_pipeline_table_name(self.output__classification_model__metrics_by_cls_on__subset),
                    TableStoreDB(
                        dbconn=ds.meta_dbconn,
                        name=get_pipeline_table_name(self.output__classification_model__metrics_by_cls_on__subset),
                        data_sql_schema=model_key_columns
                        + [
                            Column("subset_id", String, primary_key=True),
                            Column("label", String, primary_key=True),
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
            get_pipeline_table_name(self.output__classification_model__metrics_on__subset),
            Table(
                ds.get_or_create_table(
                    get_pipeline_table_name(self.output__classification_model__metrics_on__subset),
                    TableStoreDB(
                        dbconn=ds.meta_dbconn,
                        name=get_pipeline_table_name(self.output__classification_model__metrics_on__subset),
                        data_sql_schema=model_key_columns
                        + [
                            Column("subset_id", String, primary_key=True),
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

        ground_truth_inputs = build_ground_truth_batch_inputs(self.input__image__ground_truth)
        pipeline = Pipeline(
            [
                BatchTransform(
                    func=wrap_ground_truth_inputs(
                        count_classification_metrics_on_image,
                        n_ground_truth_inputs=len(ground_truth_inputs),
                        primary_keys=self.primary_keys,
                    ),
                    inputs=[
                        *ground_truth_inputs,
                        required_pipeline_input(self.input__subset__has__image),
                        required_pipeline_input(self.input__classification_prediction),
                    ],
                    outputs=[self.output__classification_model__metrics__on__image],
                    transform_keys=stable_unique(self.primary_keys + self.classification_model_primary_keys),
                    executor_config=self.executor_config,
                    labels=self.labels,
                    kwargs=dict(
                        primary_keys=self.primary_keys,
                        classification_model_primary_keys=self.classification_model_primary_keys,
                        label_mapper=self.label_mapper,
                    ),
                    chunk_size=self.chunk_size,
                    filters=self.filters,
                ),
                DatatableBatchTransform(
                    func=count_classification_metrics_on_subset,
                    inputs=[pipeline_output_as_input(self.output__classification_model__metrics__on__image)],
                    outputs=[
                        self.output__classification_model__metrics_by_cls_on__subset,
                        self.output__classification_model__metrics_on__subset,
                    ],
                    transform_keys=self.classification_model_primary_keys + ["subset_id"],
                    labels=self.labels,
                    kwargs=dict(
                        classification_model_primary_keys=self.classification_model_primary_keys,
                        pseudo_class_names=self.pseudo_class_names,
                        known_class_names=self.known_class_names,
                    ),
                    chunk_size=1,
                ),
            ]
        )
        return build_compute(ds, catalog, pipeline)
