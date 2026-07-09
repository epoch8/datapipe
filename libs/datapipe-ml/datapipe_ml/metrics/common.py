from typing import Any, Iterable, List, Sequence, Tuple

from sqlalchemy import Column, Float, func

# Storage sentinel for per-image metrics rows computed with label=None (detection-level).
# Distinct from the string "None", which may appear as a real class name in datasets.
# Do not use "\\x00": pandas treats it as NA in Series equality filters.
METRICS_NULL_LABEL = "__metrics_label_none__"

CLASS_METRIC_COLUMNS = [
    "calc__images_support",
    "calc__support",
    "calc__TP",
    "calc__FP",
    "calc__FN",
    "calc__precision",
    "calc__recall",
    "calc__f1_score",
]

OVERALL_BASE_METRIC_COLUMNS = [
    "calc__images_support",
    "calc__support",
    "calc__accuracy",
    "calc__weighted_precision",
    "calc__weighted_recall",
    "calc__weighted_f1_score",
    "calc__macro_precision",
    "calc__macro_recall",
    "calc__macro_f1_score",
    "calc__weighted_without_pseudo_classes_precision",
    "calc__weighted_without_pseudo_classes_recall",
    "calc__weighted_without_pseudo_classes_f1_score",
    "calc__macro_without_pseudo_classes_precision",
    "calc__macro_without_pseudo_classes_recall",
    "calc__macro_without_pseudo_classes_f1_score",
]

KNOWN_OVERALL_METRIC_COLUMNS = [
    "calc__weighted_known_precision",
    "calc__weighted_known_recall",
    "calc__weighted_known_f1_score",
    "calc__macro_known_precision",
    "calc__macro_known_recall",
    "calc__macro_known_f1_score",
    "calc__weighted_known_without_pseudo_classes_precision",
    "calc__weighted_known_without_pseudo_classes_recall",
    "calc__weighted_known_without_pseudo_classes_f1_score",
    "calc__macro_known_without_pseudo_classes_precision",
    "calc__macro_known_without_pseudo_classes_recall",
    "calc__macro_known_without_pseudo_classes_f1_score",
]


def stable_unique(items: Iterable[str]) -> List[str]:
    return list(dict.fromkeys(items))


def metrics_label_to_storage(label: Any) -> str:
    if label is None:
        return METRICS_NULL_LABEL
    return str(label)


def is_metrics_null_label(stored: str) -> bool:
    return stored == METRICS_NULL_LABEL


def overall_metric_columns(include_known: bool) -> List[str]:
    columns = list(OVERALL_BASE_METRIC_COLUMNS)
    if include_known:
        columns.extend(KNOWN_OVERALL_METRIC_COLUMNS)
    return columns


def float_columns(names: Sequence[str]) -> List[Column]:
    return [Column(name, Float) for name in names]


def precision_recall_f1(tp_num: Any, precision_den: Any, recall_den: Any, sql_cast: Any) -> Tuple[Any, Any, Any]:
    zero = sql_cast(0, Float)
    precision = func.coalesce(tp_num / func.nullif(precision_den, zero), zero)
    recall = func.coalesce(tp_num / func.nullif(recall_den, zero), zero)
    f1_score = func.coalesce(
        (sql_cast(2.0, Float) * precision * recall) / func.nullif(precision + recall, zero),
        zero,
    )
    return precision, recall, f1_score


def macro_weighted_metric_selects(
    source: Any, precision: Any, recall: Any, f1_score: Any, sql_cast: Any, suffix: str = ""
) -> List[Any]:
    return [
        func.avg(precision).label(f"calc__macro{suffix}_precision"),
        func.avg(recall).label(f"calc__macro{suffix}_recall"),
        func.avg(f1_score).label(f"calc__macro{suffix}_f1_score"),
        (
            func.sum(precision * sql_cast(source.c.sum_support, Float))
            / func.nullif(sql_cast(func.sum(source.c.sum_support), Float), sql_cast(0, Float))
        ).label(f"calc__weighted{suffix}_precision"),
        (
            func.sum(recall * sql_cast(source.c.sum_support, Float))
            / func.nullif(sql_cast(func.sum(source.c.sum_support), Float), sql_cast(0, Float))
        ).label(f"calc__weighted{suffix}_recall"),
        (
            func.sum(f1_score * sql_cast(source.c.sum_support, Float))
            / func.nullif(sql_cast(func.sum(source.c.sum_support), Float), sql_cast(0, Float))
        ).label(f"calc__weighted{suffix}_f1_score"),
    ]
