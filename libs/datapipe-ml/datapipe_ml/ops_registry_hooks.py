from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, Sequence

from datapipe.compute import Catalog, DatapipeApp
from datapipe.datatable import DataStore

from datapipe_ml.ops_specs import DatapipeOpsSpec

if TYPE_CHECKING:
    from datapipe_ml.spec_registry import OpsSpecRegistry


class OpsSpecsApp(Protocol):
    ops_specs: OpsSpecRegistry | None
    catalog: Catalog | None
    ds: DataStore | None


def _ensure_ops_registry(app: OpsSpecsApp) -> OpsSpecRegistry:
    from datapipe_ml.spec_registry import OpsSpecRegistry

    registry = app.ops_specs
    if registry is None:
        registry = OpsSpecRegistry()
        app.ops_specs = registry
    return registry


def ensure_datapipe_app_add_specs() -> None:
    if hasattr(DatapipeApp, "add_specs"):
        return

    def _add_specs(self: OpsSpecsApp, specs: Sequence[DatapipeOpsSpec]) -> None:
        registry = _ensure_ops_registry(self)
        previous = dict(registry._specs)
        try:
            registry.add_many(specs)
            registry.validate(self.catalog, self.ds, strict=True)
        except Exception:
            registry._specs = previous
            raise
        observability_registry = getattr(self, "observability_registry", None)
        if observability_registry is not None:
            observability_registry.attach_ops_specs(registry)

    def _get_specs(self: OpsSpecsApp) -> list[DatapipeOpsSpec]:
        return _ensure_ops_registry(self).list()

    DatapipeApp.add_specs = _add_specs  # type: ignore[attr-defined,method-assign]
    DatapipeApp.get_specs = _get_specs  # type: ignore[attr-defined,method-assign]
