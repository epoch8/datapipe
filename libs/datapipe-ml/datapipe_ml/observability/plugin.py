from __future__ import annotations

from datapipe_app.observability.registry import ObservabilityRegistry
from datapipe_ml.observability.enricher_registration import register_ops_spec_enricher
from datapipe_ml.spec_registry import OpsSpecRegistry


def register(registry: ObservabilityRegistry) -> None:
    registry.add_ops_specs_listener(register_ops_spec_enricher)
