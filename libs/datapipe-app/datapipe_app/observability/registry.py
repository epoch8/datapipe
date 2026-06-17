from __future__ import annotations

import logging
from importlib.metadata import entry_points

from datapipe_app.observability.protocols import MetricsPublisher, OverviewEnricher, StatusCollector

logger = logging.getLogger(__name__)


class ObservabilityRegistry:
    def __init__(self) -> None:
        self._publishers: list[MetricsPublisher] = []
        self._collectors: list[StatusCollector] = []
        self._enrichers: list[OverviewEnricher] = []

    @property
    def publishers(self) -> list[MetricsPublisher]:
        return list(self._publishers)

    @property
    def collectors(self) -> list[StatusCollector]:
        return list(self._collectors)

    @property
    def enrichers(self) -> list[OverviewEnricher]:
        return list(self._enrichers)

    def register_publisher(self, publisher: MetricsPublisher) -> None:
        self._publishers.append(publisher)

    def register_collector(self, collector: StatusCollector) -> None:
        self._collectors.append(collector)

    def register_overview_enricher(self, enricher: OverviewEnricher) -> None:
        self._enrichers.append(enricher)


def load_observability_plugins(registry: ObservabilityRegistry) -> None:
    try:
        eps = entry_points(group="datapipe.observability")
    except TypeError:
        eps = entry_points().get("datapipe.observability", [])
    for ep in eps:
        try:
            register_fn = ep.load()
            register_fn(registry)
            logger.info("Loaded observability plugin: %s", ep.name)
        except Exception:
            logger.exception("Failed to load observability plugin: %s", ep.name)
