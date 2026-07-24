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
class OpsDataSpec:
    tables: Sequence[str]
    item_table: str | None = None
    label_table: str | None = None
    subset_table: str | None = None
    tag_table: str | None = None


@dataclass(frozen=True)
class OpsSpecBase:
    id: str
    title: str
    description: str = ""
    icon: str = "box"
    color: str = "blue"
    data: OpsDataSpec | None = None
    relations: Sequence[OpsRelationSpec] = field(default_factory=list)
    metrics: Sequence[OpsMetricTableSpec] = field(default_factory=list)
    class_metrics: Sequence[OpsMetricTableSpec] = field(default_factory=list)
    tags: Sequence[str] = field(default_factory=list)


def ops_spec_to_dict(value: Any) -> Any:
    if is_dataclass(value) and not isinstance(value, type):
        return asdict(value)
    if isinstance(value, (list, tuple)):
        return [ops_spec_to_dict(item) for item in value]
    if isinstance(value, dict):
        return {key: ops_spec_to_dict(item) for key, item in value.items()}
    return value
