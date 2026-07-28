from __future__ import annotations

from datapipe_app.observability.plugins.registry import ObservabilityRegistry
from datapipe_app_ml_ops.observability.plugins.enricher_registration import register_ops_spec_enricher


def register(registry: ObservabilityRegistry) -> None:
    # Import registers ML spec validators on OpsSpecRegistry.
    import datapipe_app_ml_ops.ops.spec_registry  # noqa: F401

    registry.add_ops_specs_listener(register_ops_spec_enricher)
