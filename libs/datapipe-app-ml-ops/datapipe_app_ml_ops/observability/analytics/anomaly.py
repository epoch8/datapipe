from __future__ import annotations

import statistics
from typing import Optional

from datapipe_app_ml_ops.observability.schemas.models import AnomalyItem, KpiItem, MetricsRunRow


PRIMARY_METRIC_BY_TASK = {
    "detection": "mAP50_95",
    "classification": "weighted_f1_score",
    "keypoints": "pose_mAP50_95",
    "segmentation": "mAP50_95",
}

FALLBACK_METRICS = [
    "mAP50_95",
    "mAP50",
    "f1_score",
    "weighted_f1_score",
    "macro_f1_score",
    "weighted_without_pseudo_classes_f1_score",
    "macro_without_pseudo_classes_f1_score",
    "pose_mAP50_95",
]

# Support / instance counts are not comparable quality metrics; they must never be
# used to rank runs (otherwise a run with all-null metrics but a large support
# would win the "best model" selection).
COUNT_METRIC_KEYS = {
    "images_support",
    "support",
    "TP",
    "FP",
    "FN",
    "TP_extra_bbox",
    "FP_extra_bbox",
    "FN_extra_bbox",
}


def primary_metric_for_task(task_type: Optional[str], *, primary_metric: Optional[str] = None) -> str:
    if primary_metric:
        return primary_metric
    if task_type and task_type in PRIMARY_METRIC_BY_TASK:
        return PRIMARY_METRIC_BY_TASK[task_type]
    return "f1_score"


def pick_primary_value(
    metrics: dict[str, float | None],
    task_type: Optional[str],
    *,
    primary_metric: Optional[str] = None,
) -> Optional[float]:
    primary = primary_metric_for_task(task_type, primary_metric=primary_metric)
    if metrics.get(primary) is not None:
        return metrics.get(primary)
    for key in FALLBACK_METRICS:
        if metrics.get(key) is not None:
            return metrics.get(key)
    for key, v in metrics.items():
        if v is not None and key not in COUNT_METRIC_KEYS:
            return v
    return None


def compute_deltas(current: MetricsRunRow, previous: Optional[MetricsRunRow]) -> None:
    if previous is None:
        return
    for key, val in current.metrics.items():
        prev = previous.metrics.get(key)
        if val is None or prev is None:
            continue
        delta = val - prev
        current.deltas[key] = delta
        if prev != 0:
            current.delta_pct[key] = (delta / abs(prev)) * 100.0


def detect_anomalies(runs: list[MetricsRunRow], task_type: Optional[str]) -> list[AnomalyItem]:
    if len(runs) < 2:
        return []
    anomalies: list[AnomalyItem] = []
    latest, previous = runs[0], runs[1]
    primary = primary_metric_for_task(task_type)

    recall_delta = None
    latest_recall = latest.metrics.get("recall")
    previous_recall = previous.metrics.get("recall")
    if latest_recall is not None and previous_recall is not None:
        recall_delta = latest_recall - previous_recall
        if recall_delta <= -0.01:
            anomalies.append(
                AnomalyItem(
                    severity="warning",
                    metric="recall",
                    title=f"Recall dropped {recall_delta * 100:.1f}%",
                    description="Latest run recall below previous run",
                    run_id=latest.run_id,
                    value=latest.metrics.get("recall"),
                    expected=previous.metrics.get("recall"),
                    delta=recall_delta,
                )
            )

    f1_values: list[float] = [
        value for r in runs[1:11] if (value := r.metrics.get("f1_score")) is not None
    ]
    latest_f1 = latest.metrics.get("f1_score")
    if f1_values and latest_f1 is not None:
        mean = statistics.mean(f1_values)
        stdev = statistics.pstdev(f1_values) if len(f1_values) > 1 else 0.0
        threshold = mean - 2 * stdev
        if latest_f1 < threshold:
            anomalies.append(
                AnomalyItem(
                    severity="warning",
                    metric="f1_score",
                    title="F1 below rolling trend",
                    description=f"Latest F1 {latest_f1:.3f} below mean−2σ ({threshold:.3f})",
                    run_id=latest.run_id,
                    value=latest_f1,
                    expected=mean,
                )
            )

    latest_primary = latest.metrics.get(primary)
    previous_primary = previous.metrics.get(primary)
    if latest_primary is not None and previous_primary is not None:
        if latest_primary < previous_primary:
            anomalies.append(
                AnomalyItem(
                    severity="info",
                    metric=primary,
                    title=f"{primary} regression",
                    description="Primary metric decreased vs previous run",
                    run_id=latest.run_id,
                    delta=latest_primary - previous_primary,
                )
            )

    support_latest = latest.metrics.get("support")
    support_prev = previous.metrics.get("support")
    if support_latest and support_prev and abs(support_latest - support_prev) / support_prev > 0.2:
        anomalies.append(
            AnomalyItem(
                severity="info",
                metric="support",
                title="Support changed significantly",
                description="Instance support differs >20% from previous run",
                run_id=latest.run_id,
            )
        )

    return anomalies


def build_kpis(runs: list[MetricsRunRow], task_type: Optional[str], best: Optional[MetricsRunRow]) -> list[KpiItem]:
    if not runs:
        return []
    latest = runs[0]
    kpi_defs = [
        ("mAP50", "mAP50"),
        ("mAP50_95", "mAP50-95"),
        ("weighted_f1_score", "Weighted F1"),
        ("macro_f1_score", "Macro F1"),
        ("weighted_precision", "Weighted Precision"),
        ("weighted_recall", "Weighted Recall"),
        ("accuracy", "Accuracy"),
        ("precision", "Precision"),
        ("recall", "Recall"),
        ("f1_score", "F1 Score"),
        ("iou_mean", "IoU mean"),
    ]
    seen_labels: set[str] = set()
    kpis: list[KpiItem] = []
    for key, label in kpi_defs:
        if latest.metrics.get(key) is None:
            continue
        if label in seen_labels:
            continue
        seen_labels.add(label)
        trend = [
            {"x": r.run_id, "y": r.metrics.get(key)}
            for r in reversed(runs[:10])
            if r.metrics.get(key) is not None
        ]
        kpis.append(
            KpiItem(
                key=key,
                label=label,
                value=latest.metrics.get(key),
                delta_pct=latest.delta_pct.get(key),
                format="integer" if key == "images_support" else "float",
                higher_is_better=True,
                trend=trend,
            )
        )
        if len(kpis) >= 6:
            break
    return kpis
