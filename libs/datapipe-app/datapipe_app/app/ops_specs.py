from __future__ import annotations

from typing import TYPE_CHECKING, Sequence

from datapipe.compute import Catalog
from datapipe.datatable import DataStore

from datapipe_app.ops.spec_registry import OpsSpecRegistry
from datapipe_app.ops.specs import OpsSpecBase

if TYPE_CHECKING:
    from datapipe_app.observability.plugins.registry import ObservabilityRegistry


class OpsSpecsMixin:
    ops_specs: OpsSpecRegistry | None
    catalog: Catalog | None
    ds: DataStore | None
    observability_registry: ObservabilityRegistry | None

    def _ensure_ops_registry(self) -> OpsSpecRegistry:
        registry = self.ops_specs
        if registry is None:
            registry = OpsSpecRegistry()
            self.ops_specs = registry
        return registry

    def add_specs(self, specs: Sequence[OpsSpecBase]) -> None:
        registry = self._ensure_ops_registry()
        previous = dict(registry._specs)
        try:
            registry.add_many(specs)
            registry.validate(self.catalog, self.ds, strict=True)
        except Exception:
            registry._specs = previous
            raise
        if self.observability_registry is not None:
            self.observability_registry.attach_ops_specs(registry)

    def get_specs(self) -> list[OpsSpecBase]:
        return self._ensure_ops_registry().list()
