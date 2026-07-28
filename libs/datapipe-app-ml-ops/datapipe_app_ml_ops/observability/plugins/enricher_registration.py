from __future__ import annotations

from typing import cast

from datapipe_app.observability.plugins.registry import ObservabilityRegistry
from datapipe_app.ops.spec_registry import OpsSpecRegistry as BaseOpsSpecRegistry
from datapipe_app_ml_ops.observability.plugins.ops_spec_enricher import OpsSpecOverviewEnricher
from datapipe_app_ml_ops.ops.spec_registry import OpsSpecRegistry


def register_ops_spec_enricher(
    registry: ObservabilityRegistry,
    ops_specs: BaseOpsSpecRegistry,
) -> None:
    ml_ops_specs = cast(OpsSpecRegistry, ops_specs)
    if not ml_ops_specs.list():
        return
    for enricher in registry.enrichers:
        if isinstance(enricher, OpsSpecOverviewEnricher):
            return
    registry.register_overview_enricher(OpsSpecOverviewEnricher(ml_ops_specs))
