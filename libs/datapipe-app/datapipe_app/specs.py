from __future__ import annotations

from dataclasses import asdict, dataclass, field, is_dataclass
from typing import Any, Literal, Sequence


MetricDirection = Literal["max", "min"]
ColumnKind = Literal["text", "number", "datetime", "duration", "status", "link", "chip", "split", "models_count"]
SnapshotLabelMode = Literal["id", "short_id", "timestamp"]
OpsFilterOperator = Literal[
    "contains",
    "not_contains",
    "regex",
    "equal",
    "not_equal",
    "is_empty",
]
RecordViewKind = Literal["image", "text"]
ImageOverlayRole = Literal["gt", "prediction", "record"]

# Must match datapipe_ml.metrics.common.METRICS_NULL_LABEL
METRICS_NULL_LABEL = "__metrics_label_none__"


@dataclass(frozen=True)
class OpsFilterRule:
    column_id: str
    operator: OpsFilterOperator
    value: str | None = None


@dataclass(frozen=True)
class OpsColumn:
    id: str
    label: str
    source: str
    kind: ColumnKind = "text"
    fmt: str | None = None
    sortable: bool = True
    filterable: bool = False
    link_to: str | None = None
    width: int | None = None


@dataclass(frozen=True)
class OpsColumnGroup:
    label: str
    columns: Sequence[OpsColumn]


@dataclass(frozen=True)
class OpsTableRef:
    table: str
    id_column: str | None = None
    created_at_column: str | None = None
    display_name_column: str | None = None


@dataclass(frozen=True)
class OpsImageAnnotationSpec:
    table: str
    primary_key_columns: Sequence[str]
    bboxes_column: str | None = "bboxes"
    labels_column: str | None = "labels"
    scores_column: str | None = None
    annotation_column: str | None = None
    join_columns: dict[str, str] = field(default_factory=dict)
    role: ImageOverlayRole = "record"


@dataclass(frozen=True)
class OpsImageDataSpec:
    kind: Literal["image"] = "image"
    image_table: str = ""
    image_primary_key_columns: Sequence[str] = field(default_factory=tuple)
    image_url_column: str = "image_url"
    subset_table: str | None = None
    subset_join_columns: dict[str, str] = field(default_factory=dict)
    subset_column: str | None = "subset_id"
    ground_truth: OpsImageAnnotationSpec | None = None
    visualizer: str | None = None
    preview_size: int = 84
    modal_max_side: int = 1100
    detail_max_side: int = 1600


@dataclass(frozen=True)
class OpsImageRecordViewSpec:
    kind: Literal["image"] = "image"
    table: str = ""
    scope_column: str | None = None
    primary_key_columns: Sequence[str] = field(default_factory=tuple)
    image_url_column: str | None = "image_url"
    image_url_table: str | None = None
    image_url_join_columns: dict[str, str] = field(default_factory=dict)
    bboxes_column: str | None = "bboxes"
    labels_column: str | None = "labels"
    scores_column: str | None = None
    width_column: str | None = None
    height_column: str | None = None
    visualizer: str | None = None
    preview_size: int = 84
    modal_max_side: int = 1100
    detail_max_side: int = 1600


@dataclass(frozen=True)
class OpsImagePredictionViewSpec:
    kind: Literal["image"] = "image"
    table: str = ""
    model_id_column: str = ""
    image_primary_key_columns: Sequence[str] = field(default_factory=tuple)
    image_url_table: str = ""
    image_url_column: str = "image_url"
    image_url_join_columns: dict[str, str] = field(default_factory=dict)
    prediction: OpsImageAnnotationSpec | None = None
    ground_truth: OpsImageAnnotationSpec | None = None
    subset_table: str | None = None
    subset_join_columns: dict[str, str] = field(default_factory=dict)
    subset_column: str | None = "subset_id"
    metrics_on_image: OpsMetricTableSpec | None = None
    metrics_on_image_label: str = METRICS_NULL_LABEL
    visualizer: str | None = None
    preview_size: int = 84
    modal_max_side: int = 1100
    detail_max_side: int = 1600


@dataclass(frozen=True)
class OpsTextRecordViewSpec:
    kind: Literal["text"] = "text"
    table: str = ""
    scope_column: str | None = None
    primary_key_columns: Sequence[str] = field(default_factory=tuple)


@dataclass(frozen=True)
class OpsFrozenDatasetSpec:
    table: str
    id_column: str
    created_at_column: str
    display_name_column: str | None = None
    label_mode: SnapshotLabelMode = "timestamp"
    split_columns: dict[str, str] = field(default_factory=dict)
    models_count_relation_id: str | None = "model_trained_on_frozen_dataset"
    record_view: OpsImageRecordViewSpec | OpsTextRecordViewSpec | None = None
    columns: Sequence[OpsColumn] = field(default_factory=list)
    default_sort: Sequence[tuple[str, Literal["asc", "desc"]]] = field(default_factory=list)


@dataclass(frozen=True)
class OpsModelSpec:
    table: str
    id_column: str
    display_name_column: str | None = None
    created_at_column: str | None = None
    artifact_uri_column: str | None = None
    is_best_table: str | None = None
    is_best_column: str | None = None
    prediction_view: OpsImagePredictionViewSpec | None = None


@dataclass(frozen=True)
class OpsTrainingSpec:
    status_table: str
    artifact_columns: dict[str, str] = field(default_factory=dict)
    columns: Sequence[OpsColumn] = field(default_factory=list)
    default_sort: Sequence[tuple[str, Literal["asc", "desc"]]] = field(default_factory=list)


@dataclass(frozen=True)
class OpsRelationSpec:
    id: str
    table: str
    from_entity: str
    from_column: str
    to_entity: str
    to_column: str


@dataclass(frozen=True)
class OpsMetricTableSpec:
    id: str
    title: str
    table: str
    metric_source: str
    primary_key_columns: Sequence[str]
    entity_links: dict[str, str]
    primary_columns: Sequence[OpsColumn]
    metric_columns: Sequence[OpsColumn | OpsColumnGroup]
    best_metric_column: str | None = None
    best_metric_direction: MetricDirection = "max"
    default_sort: Sequence[tuple[str, Literal["asc", "desc"]]] = field(default_factory=list)
    filters: Sequence[OpsColumn] = field(default_factory=list)
    default_filters: Sequence[OpsFilterRule] = field(default_factory=list)


@dataclass(frozen=True)
class OpsClassMetricTableSpec(OpsMetricTableSpec):
    pass


@dataclass(frozen=True)
class OpsDataSpec:
    tables: Sequence[str]
    item_table: str | None = None
    label_table: str | None = None
    subset_table: str | None = None
    tag_table: str | None = None
    image_view: OpsImageDataSpec | None = None


@dataclass(frozen=True)
class DatapipeOpsSpec:
    id: str
    title: str
    description: str = ""
    icon: str = "box"
    color: str = "blue"
    data: OpsDataSpec | None = None
    frozen_dataset: OpsFrozenDatasetSpec | None = None
    model: OpsModelSpec | None = None
    training: OpsTrainingSpec | None = None
    relations: Sequence[OpsRelationSpec] = field(default_factory=list)
    metrics: Sequence[OpsMetricTableSpec] = field(default_factory=list)
    class_metrics: Sequence[OpsClassMetricTableSpec] = field(default_factory=list)
    tags: Sequence[str] = field(default_factory=list)


def ops_spec_to_dict(value: Any) -> Any:
    if is_dataclass(value) and not isinstance(value, type):
        return asdict(value)
    if isinstance(value, (list, tuple)):
        return [ops_spec_to_dict(item) for item in value]
    if isinstance(value, dict):
        return {key: ops_spec_to_dict(item) for key, item in value.items()}
    return value
