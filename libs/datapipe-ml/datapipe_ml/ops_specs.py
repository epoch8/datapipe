from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal, Sequence

from datapipe_app.specs import (
    OpsSpecBase,
    OpsColumn,
    OpsDataSpec as OpsDataSpecBase,
    OpsMetricTableSpec,
    SnapshotLabelMode,
)
from datapipe_ml.metrics.common import METRICS_NULL_LABEL

ImageOverlayRole = Literal["gt", "prediction", "record"]
RecordViewKind = Literal["image", "text"]


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
    records_show_subset: bool = False
    records_show_ground_truth: bool = False
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
class OpsClassMetricTableSpec(OpsMetricTableSpec):
    pass


@dataclass(frozen=True)
class OpsDataSpec(OpsDataSpecBase):
    image_view: OpsImageDataSpec | None = None


@dataclass(frozen=True)
class DatapipeOpsSpec(OpsSpecBase):
    data: OpsDataSpec | None = None
    frozen_dataset: OpsFrozenDatasetSpec | None = None
    model: OpsModelSpec | None = None
    training: OpsTrainingSpec | None = None
    class_metrics: Sequence[OpsClassMetricTableSpec] = field(default_factory=list)


__all__ = [
    "METRICS_NULL_LABEL",
    "DatapipeOpsSpec",
    "OpsClassMetricTableSpec",
    "OpsDataSpec",
    "OpsFrozenDatasetSpec",
    "OpsImageAnnotationSpec",
    "OpsImageDataSpec",
    "OpsImagePredictionViewSpec",
    "OpsImageRecordViewSpec",
    "OpsModelSpec",
    "OpsTextRecordViewSpec",
    "OpsTrainingSpec",
]
