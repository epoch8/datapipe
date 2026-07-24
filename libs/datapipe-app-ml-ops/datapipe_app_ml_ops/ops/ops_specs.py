from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal, Sequence

from datapipe.types import Labels
from datapipe_app.ops.specs import (
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
    """Describes the frozen-dataset / snapshot table.

    ``run_labels`` selects pipeline steps for ``Freeze new dataset`` (typically
    ``("stage", "train-prepare")``). Leave empty (the default) to disable the
    action in the UI and API.
    """

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
    run_labels: Labels = field(default_factory=list)


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
class OpsTrainConfigRegistrySpec:
    """Describes the train config registry tables (spec §13).

    * ``table`` — API-owned custom experiments (writes go here).
    * ``default_table`` — optional pipeline-owned built-in presets (read-only).

    When ``default_table`` is set, list/get merge rows from both tables and force
    ``source="builtin"`` / ``source="custom"`` based on which table the row came
    from (the tables themselves do not store ``source``).
    """

    table: str
    id_column: str
    params_column: str
    config_type: str
    default_table: str | None = None
    source_column: str = "train_config__source"
    display_name_column: str = "train_config__display_name"
    description_column: str = "train_config__description"
    config_type_column: str = "train_config__config_type"
    hash_column: str = "train_config__config_hash"
    active_column: str = "train_config__is_active"
    revision_column: str = "train_config__revision"
    created_at_column: str = "train_config__created_at"
    updated_at_column: str = "train_config__updated_at"


@dataclass(frozen=True)
class OpsTrainingRequestSpec:
    """Describes the training request table (spec §13).

    ``run_labels`` selects the pipeline steps to launch for a request, and
    launching applies a primary-key filter ``{id_column: [request_id]}`` so only
    the requested row is materialized (spec §18).

    Leave ``run_labels`` empty (the default) when immediate launch from the UI
    is not configured. Typical setup uses a dedicated stage such as
    ``("stage", "train-without-freeze")`` so freeze/split steps are not run.

    ``max_within_time`` mirrors the training step freshness window (e.g.
    ``"1w"``): the Ops UI hides frozen datasets older than this relative to the
    latest snapshot unless the user opts into bypass (``force=True``).
    """

    table: str
    id_column: str
    train_config_id_column: str
    frozen_dataset_id_column: str
    run_labels: Labels = field(default_factory=list)
    # Freshness window for UI dataset filtering and non-forced requests (e.g. "1w").
    max_within_time: str | None = None
    kind_column: str = "training_request__kind"
    enabled_column: str = "training_request__enabled"
    force_column: str = "training_request__force"
    max_within_time_column: str = "training_request__max_within_time"
    config_source_column: str = "training_request__config_source"
    config_name_snapshot_column: str = "training_request__config_name_snapshot"
    config_params_snapshot_column: str = "training_request__config_params_snapshot"
    config_hash_column: str = "training_request__config_hash"
    requested_at_column: str = "training_request__requested_at"
    requested_by_column: str = "training_request__requested_by"
    client_request_id_column: str = "training_request__client_request_id"
    # Optional join to a run/status table used to compute ``runs_count``.
    status_table: str | None = None
    status_request_id_column: str | None = None


@dataclass(frozen=True)
class OpsTrainingSpec:
    status_table: str
    artifact_columns: dict[str, str] = field(default_factory=dict)
    columns: Sequence[OpsColumn] = field(default_factory=list)
    default_sort: Sequence[tuple[str, Literal["asc", "desc"]]] = field(default_factory=list)
    experiments: OpsTrainConfigRegistrySpec | None = None
    requests: OpsTrainingRequestSpec | None = None


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
    "OpsTrainConfigRegistrySpec",
    "OpsTrainingRequestSpec",
    "OpsTrainingSpec",
]
