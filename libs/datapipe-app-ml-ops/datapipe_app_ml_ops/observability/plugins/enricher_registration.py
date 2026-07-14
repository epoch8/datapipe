from __future__ import annotations

from datapipe_app.observability.plugins.registry import ObservabilityRegistry
from datapipe_app_ml_ops.observability.plugins.ops_spec_enricher import OpsSpecOverviewEnricher
from datapipe_app_ml_ops.ops.spec_registry import OpsSpecRegistry


def register_ops_spec_enricher(
    registry: ObservabilityRegistry,
    ops_specs: OpsSpecRegistry,
) -> None:
    if not ops_specs.list():
        return
    for enricher in registry.enrichers:
        if isinstance(enricher, OpsSpecOverviewEnricher):
            return
    registry.register_overview_enricher(OpsSpecOverviewEnricher(ops_specs))
