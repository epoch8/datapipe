from __future__ import annotations

from dataclasses import dataclass, fields
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datapipe.compute import Catalog


@dataclass(frozen=True)
class ObservabilityTableConfig:
    """Physical table names owned by datapipe-app (runs, logs, registry, analytics)."""

    pipeline_registry: str = "pipeline_registry"
    pipeline_runs: str = "pipeline_runs"
    pipeline_run_steps: str = "pipeline_run_steps"
    pipeline_run_logs: str = "pipeline_run_logs"
    pipeline_schedules: str = "pipeline_schedules"
    analytics_metrics_on_subset: str = "analytics_metrics_on_subset"
    analytics_metrics_by_class: str = "analytics_metrics_by_class"
    analytics_training_runs: str = "analytics_training_runs"

    def __post_init__(self) -> None:
        validate_observability_table_names(self)

    def table_names(self) -> dict[str, str]:
        return {field.name: getattr(self, field.name) for field in fields(self)}


def validate_observability_table_names(config: ObservabilityTableConfig) -> None:
    seen: dict[str, str] = {}
    for field_name, table_name in config.table_names().items():
        if table_name in seen:
            raise ValueError(
                f"Duplicate datapipe-app observability table name {table_name!r}: "
                f"configured for both {seen[table_name]} and {field_name}"
            )
        seen[table_name] = field_name


def validate_observability_tables_against_catalog(
    config: ObservabilityTableConfig,
    catalog: Catalog,
) -> None:
    validate_observability_table_names(config)
    observability_names = set(config.table_names().values())
    pipeline_names = _collect_pipeline_table_names(catalog)
    overlap = observability_names & pipeline_names
    if overlap:
        sorted_overlap = ", ".join(sorted(overlap))
        raise ValueError(
            "Datapipe-app observability tables must not reuse pipeline catalog table names: "
            f"{sorted_overlap}"
        )


def _collect_pipeline_table_names(catalog: Catalog) -> set[str]:
    names: set[str] = set()
    for catalog_name, table in catalog.catalog.items():
        names.add(catalog_name)
        store_name = getattr(table.store, "name", None)
        if isinstance(store_name, str):
            names.add(store_name)
    return names
