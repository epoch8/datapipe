from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

from datapipe.compute import Catalog, DataStore
from datapipe.datatable import DataStore as DS  # noqa: F401

from datapipe_app.observability.db import ObservabilityStore


@runtime_checkable
class MetricsPublisher(Protocol):
    def publish_metrics(
        self,
        store: ObservabilityStore,
        *,
        pipeline_id: str,
        ds: DataStore,
        catalog: Catalog,
    ) -> None: ...


@runtime_checkable
class StatusCollector(Protocol):
    def collect_pipeline_status(
        self,
        *,
        pipeline_id: str,
        ds: DataStore | None,
        catalog: Catalog | None,
    ) -> list[dict[str, Any]]: ...


@runtime_checkable
class OverviewEnricher(Protocol):
    def enrich_overview_card(
        self,
        *,
        pipeline_id: str,
        ds: DataStore | None,
        catalog: Catalog | None,
        store: ObservabilityStore,
    ) -> dict[str, Any] | None: ...

    def enrich_pipeline_detail(
        self,
        *,
        pipeline_id: str,
        ds: DataStore | None,
        catalog: Catalog | None,
        store: ObservabilityStore,
    ) -> list[dict[str, Any]]: ...
