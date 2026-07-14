from __future__ import annotations

from datapipe_app.observability.plugins.registry import ObservabilityRegistry
from datapipe_app_ml_ops.observability.plugins.enricher_registration import register_ops_spec_enricher
from datapipe_app_ml_ops.ops.spec_registry import OpsSpecRegistry


def register(registry: ObservabilityRegistry) -> None:
    registry.add_ops_specs_listener(register_ops_spec_enricher)
