from __future__ import annotations

from typing import Sequence

from datapipe_app.specs import DatapipeOpsSpec, OpsColumn, OpsImageRecordViewSpec, OpsTextRecordViewSpec

COMPUTED_SOURCES = frozenset({"split", "models_count", "duration_seconds"})
COMPUTED_KINDS = frozenset({"split", "models_count"})


def column_source_by_link(columns: Sequence[OpsColumn], link_to: str) -> str | None:
    for column in columns:
        if column.link_to == link_to and column.source:
            return column.source
    return None


def is_db_column(column: OpsColumn) -> bool:
    if column.kind in COMPUTED_KINDS:
        return False
    return column.source not in COMPUTED_SOURCES


def query_columns(columns: Sequence[OpsColumn]) -> list[OpsColumn]:
    return [column for column in columns if is_db_column(column)]


def entity_links_from_columns(columns: Sequence[OpsColumn]) -> dict[str, str]:
    links: dict[str, str] = {}
    for column in columns:
        if column.link_to and column.source:
            links[column.link_to] = column.source
    return links


def column_by_id(columns: Sequence[OpsColumn], column_id: str) -> OpsColumn | None:
    for column in columns:
        if column.id == column_id:
            return column
    return None


def column_by_kind(columns: Sequence[OpsColumn], kind: str) -> OpsColumn | None:
    for column in columns:
        if column.kind == kind:
            return column
    return None


def column_by_source(columns: Sequence[OpsColumn], source: str) -> OpsColumn | None:
    for column in columns:
        if column.source == source:
            return column
    return None


def resolve_frozen_scope_column(
    spec: DatapipeOpsSpec,
    view: OpsImageRecordViewSpec | OpsTextRecordViewSpec,
) -> str:
    if view.scope_column:
        return view.scope_column
    if spec.frozen_dataset is not None:
        return spec.frozen_dataset.id_column
    raise ValueError("frozen_dataset.record_view.scope_column is not configured and frozen_dataset.id_column is missing")
