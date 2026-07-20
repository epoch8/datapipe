from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING, Protocol, runtime_checkable

from datapipe.store.database import DBConn

from datapipe_app.observability.connections.dbconn import dbconn_same_target

if TYPE_CHECKING:
    from datapipe.compute import Catalog


@dataclass(frozen=True)
class ObservabilityTableConfig:
    """Physical table names owned by datapipe-app (runs, registry, analytics)."""

    pipeline_registry: str = "datapipe_api__registry"
    pipeline_runs: str = "datapipe_api__runs"
    pipeline_run_steps: str = "datapipe_api__run_steps"

    def __post_init__(self) -> None:
        validate_observability_table_names(self)

    def table_names(self) -> dict[str, str]:
        return asdict(self)


@runtime_checkable
class _ObservabilityTableNames(Protocol):
    def table_names(self) -> dict[str, str]: ...


@runtime_checkable
class _NamedTableStore(Protocol):
    name: str


def validate_observability_table_names(config: _ObservabilityTableNames) -> None:
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


def ensure_observability_tables_compatible_with_pipeline(
    *,
    observability_dbconn: DBConn,
    pipeline_dbconn: DBConn,
    config: ObservabilityTableConfig,
    catalog: Catalog,
) -> None:
    """Reject observability table names that would collide with the pipeline catalog."""
    if not dbconn_same_target(observability_dbconn, pipeline_dbconn):
        return
    if not catalog.catalog:
        return
    validate_observability_tables_against_catalog(config, catalog)


def _collect_pipeline_table_names(catalog: Catalog) -> set[str]:
    names: set[str] = set()
    for catalog_name, table in catalog.catalog.items():
        names.add(catalog_name)
        store_name = table.store.name if isinstance(table.store, _NamedTableStore) else None
        if isinstance(store_name, str):
            names.add(store_name)
    return names
