from __future__ import annotations

import logging
from collections.abc import Callable
from importlib.metadata import entry_points
from typing import TYPE_CHECKING

from datapipe_app.observability.plugins.protocols import OverviewEnricher, StatusCollector

if TYPE_CHECKING:
    from datapipe_app_ml_ops.ops.spec_registry import OpsSpecRegistry

logger = logging.getLogger(__name__)

OpsSpecsListener = Callable[["ObservabilityRegistry", "OpsSpecRegistry"], None]


class ObservabilityRegistry:
    def __init__(self) -> None:
        self._collectors: list[StatusCollector] = []
        self._enrichers: list[OverviewEnricher] = []
        self._ops_specs_listeners: list[OpsSpecsListener] = []
        self.ops_specs: OpsSpecRegistry | None = None

    @property
    def collectors(self) -> list[StatusCollector]:
        return list(self._collectors)

    @property
    def enrichers(self) -> list[OverviewEnricher]:
        return list(self._enrichers)

    def register_collector(self, collector: StatusCollector) -> None:
        self._collectors.append(collector)

    def register_overview_enricher(self, enricher: OverviewEnricher) -> None:
        self._enrichers.append(enricher)

    def add_ops_specs_listener(self, listener: OpsSpecsListener) -> None:
        self._ops_specs_listeners.append(listener)
        if self.ops_specs is not None:
            listener(self, self.ops_specs)

    def attach_ops_specs(self, ops_specs: OpsSpecRegistry) -> None:
        self.ops_specs = ops_specs
        for listener in self._ops_specs_listeners:
            listener(self, ops_specs)


def load_observability_plugins(registry: ObservabilityRegistry) -> None:
    try:
        eps = entry_points(group="datapipe.observability")
    except TypeError:
        eps = entry_points().select(group="datapipe.observability")
    for ep in eps:
        try:
            register_fn = ep.load()
            register_fn(registry)
            logger.info("Loaded observability plugin: %s", ep.name)
        except Exception:
            logger.exception("Failed to load observability plugin: %s", ep.name)
