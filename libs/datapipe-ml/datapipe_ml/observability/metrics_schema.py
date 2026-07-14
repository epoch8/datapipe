from __future__ import annotations

from datapipe_ml.observability.schemas import MetricColumnGroup, MetricDefinition, MetricsTableSchema

_COUNT_KEYS = frozenset(
    {
        "images_support",
        "support",
        "TP",
        "FP",
        "FN",
        "TP_extra_bbox",
        "FP_extra_bbox",
        "FN_extra_bbox",
        "images",
        "objects",
        "detections",
        "false_positives",
        "false_negatives",
    }
)


def _def(
    key: str,
    label: str,
    short: str,
    group: str,
    *,
    task_types: list[str] | None = None,
    fmt: str = "float",
    visible: bool = True,
    primary: bool = False,
    higher: bool = True,
) -> MetricDefinition:
    return MetricDefinition(
        key=key,
        label=label,
        short_label=short,
        group=group,
        task_types=task_types or ["*"],
        format=fmt,
        higher_is_better=higher,
        visible_by_default=visible,
        primary=primary,
    )


def _classification_groups() -> list[MetricColumnGroup]:
    return [
        MetricColumnGroup(
            key="core",
            label="Core",
            priority=1,
            metrics=[
                _def("weighted_f1_score", "Primary F1", "Primary", "core", primary=True),
                _def("accuracy", "Accuracy", "Acc", "core"),
                _def("support", "Support", "Sup", "core", fmt="integer"),
            ],
        ),
        MetricColumnGroup(
            key="weighted",
            label="Weighted",
            priority=2,
            metrics=[
                _def("weighted_f1_score", "Weighted F1", "F1", "weighted"),
                _def("weighted_precision", "Weighted Precision", "P", "weighted"),
                _def("weighted_recall", "Weighted Recall", "R", "weighted"),
            ],
        ),
        MetricColumnGroup(
            key="macro",
            label="Macro",
            priority=3,
            metrics=[
                _def("macro_f1_score", "Macro F1", "F1", "macro"),
                _def("macro_precision", "Macro Precision", "P", "macro"),
                _def("macro_recall", "Macro Recall", "R", "macro"),
            ],
        ),
        MetricColumnGroup(
            key="no_pseudo",
            label="No pseudo",
            priority=4,
            metrics=[
                _def("weighted_without_pseudo_classes_f1_score", "Weighted F1", "F1", "no_pseudo", visible=False),
                _def("weighted_without_pseudo_classes_precision", "Weighted P", "P", "no_pseudo", visible=False),
                _def("weighted_without_pseudo_classes_recall", "Weighted R", "R", "no_pseudo", visible=False),
                _def("macro_without_pseudo_classes_f1_score", "Macro F1", "F1", "no_pseudo", visible=False),
                _def("macro_without_pseudo_classes_precision", "Macro P", "P", "no_pseudo", visible=False),
                _def("macro_without_pseudo_classes_recall", "Macro R", "R", "no_pseudo", visible=False),
            ],
        ),
    ]


def _detection_groups() -> list[MetricColumnGroup]:
    return [
        MetricColumnGroup(
            key="core",
            label="Core",
            priority=1,
            metrics=[
                _def("mAP50_95", "mAP50-95", "mAP", "core", task_types=["detection"], primary=True),
                _def("mAP50", "mAP50", "mAP50", "core", task_types=["detection"]),
                _def("support", "Support", "Sup", "core", fmt="integer"),
            ],
        ),
        MetricColumnGroup(
            key="weighted",
            label="Weighted",
            priority=2,
            metrics=[
                _def("weighted_f1_score", "Weighted F1", "F1", "weighted"),
                _def("weighted_precision", "Weighted Precision", "P", "weighted"),
                _def("weighted_recall", "Weighted Recall", "R", "weighted"),
            ],
        ),
        MetricColumnGroup(
            key="macro",
            label="Macro",
            priority=3,
            metrics=[
                _def("macro_f1_score", "Macro F1", "F1", "macro"),
                _def("macro_precision", "Macro Precision", "P", "macro"),
                _def("macro_recall", "Macro Recall", "R", "macro"),
            ],
        ),
        MetricColumnGroup(
            key="quality",
            label="Detection",
            priority=4,
            metrics=[
                _def("precision", "Precision", "P", "quality", task_types=["detection"]),
                _def("recall", "Recall", "R", "quality", task_types=["detection"]),
                _def("f1_score", "F1", "F1", "quality", task_types=["detection"]),
            ],
        ),
        MetricColumnGroup(
            key="localization",
            label="Localization",
            priority=5,
            metrics=[
                _def("iou_mean", "Mean IoU", "IoU", "localization", task_types=["detection"], visible=False),
            ],
        ),
    ]


def _segmentation_groups() -> list[MetricColumnGroup]:
    return [
        MetricColumnGroup(
            key="core",
            label="Core",
            priority=1,
            metrics=[
                _def("iou_mean", "mIoU", "mIoU", "core", task_types=["segmentation"], primary=True),
                _def("dice", "Dice", "Dice", "core", task_types=["segmentation"]),
                _def("pixel_accuracy", "Pixel accuracy", "PixAcc", "core", task_types=["segmentation"], visible=False),
                _def("support", "Support", "Sup", "core", fmt="integer"),
            ],
        ),
    ]


def _keypoints_groups() -> list[MetricColumnGroup]:
    return [
        MetricColumnGroup(
            key="core",
            label="Core",
            priority=1,
            metrics=[
                _def("pose_mAP50_95", "Pose mAP50-95", "mAP", "core", task_types=["keypoints"], primary=True),
                _def("pose_mAP50", "Pose mAP50", "mAP50", "core", task_types=["keypoints"]),
                _def("support", "Support", "Sup", "core", fmt="integer"),
            ],
        ),
        MetricColumnGroup(
            key="pose",
            label="Pose",
            priority=2,
            metrics=[
                _def("pose_P", "Pose precision", "P", "pose", task_types=["keypoints"]),
                _def("pose_R", "Pose recall", "R", "pose", task_types=["keypoints"]),
                _def("OKS", "OKS", "OKS", "pose", task_types=["keypoints"], visible=False),
                _def("PCK", "PCK", "PCK", "pose", task_types=["keypoints"], visible=False),
            ],
        ),
    ]


def infer_task_type_from_metrics(available: set[str]) -> str | None:
    if available & {"mAP50", "mAP50_95"}:
        return "detection"
    if any(k.startswith("pose_") for k in available):
        return "keypoints"
    if available & {
        "weighted_f1_score",
        "macro_f1_score",
        "weighted_precision",
        "macro_precision",
        "accuracy",
    }:
        return "classification"
    return None


def _heuristic_group(key: str) -> str:
    lk = key.lower()
    if any(x in lk for x in ("support", "count", "items", "images", "objects")):
        return "counts"
    if any(x in lk for x in ("map", "iou", "dice", "oks", "pck", "hausdorff")):
        return "task_specific"
    if any(x in lk for x in ("f1", "precision", "recall", "accuracy")):
        return "quality"
    return "other"


def _fallback_groups(available: set[str], primary_metric: str) -> list[MetricColumnGroup]:
    by_group: dict[str, list[MetricDefinition]] = {}
    for key in sorted(available):
        if key in _COUNT_KEYS:
            continue
        group = _heuristic_group(key)
        by_group.setdefault(group, []).append(
            _def(key, key.replace("_", " ").title(), key[:6], group, primary=(key == primary_metric))
        )
    return [
        MetricColumnGroup(key=g, label=g.replace("_", " ").title(), priority=i + 1, metrics=metrics)
        for i, (g, metrics) in enumerate(sorted(by_group.items(), key=lambda x: x[0]))
    ]


def build_metric_schema(
    task_type: str | None,
    available_metrics: list[str] | set[str],
    *,
    primary_metric: str | None = None,
) -> MetricsTableSchema:
    tt = (task_type or "custom").lower()
    available = set(available_metrics)
    if tt == "custom":
        inferred = infer_task_type_from_metrics(available)
        if inferred:
            tt = inferred
    if tt in ("classification", "cls"):
        groups = _classification_groups()
        pm = primary_metric or "weighted_f1_score"
    elif tt == "detection":
        groups = _detection_groups()
        pm = primary_metric or "mAP50_95"
    elif tt == "segmentation":
        groups = _segmentation_groups()
        pm = primary_metric or "iou_mean"
    elif tt in ("keypoints", "pose"):
        groups = _keypoints_groups()
        pm = primary_metric or "pose_mAP50_95"
    else:
        pm = primary_metric or (next(iter(available), None) or "f1_score")
        groups = _fallback_groups(available, pm)

    filtered: list[MetricColumnGroup] = []
    seen_keys: set[str] = set()
    for group in groups:
        metrics = []
        for m in group.metrics:
            if m.key in seen_keys:
                continue
            if available and m.key not in available:
                continue
            seen_keys.add(m.key)
            metrics.append(m)
        if metrics:
            filtered.append(group.model_copy(update={"metrics": metrics}))

    other_keys = sorted(k for k in available if k not in seen_keys and k not in _COUNT_KEYS)
    if other_keys:
        filtered.append(
            MetricColumnGroup(
                key="other",
                label="Other",
                priority=99,
                metrics=[_def(k, k, k[:6], "other", visible=False) for k in other_keys],
            )
        )

    return MetricsTableSchema(task_type=tt, primary_metric=pm, groups=filtered)
