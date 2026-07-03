from __future__ import annotations

from datapipe_app.observability.registry import ObservabilityRegistry

from datapipe_ml.observability.hooks import register_training_curve_hook
from datapipe_ml.observability.metrics import MLMetricsPublisher
from datapipe_ml.observability.overview import MLOverviewEnricher
from datapipe_ml.observability.training import TrainingStatusCollector
from datapipe_ml.observability.training_curves import TrainingCurvePublisher


def register(registry: ObservabilityRegistry) -> None:
    registry.register_publisher(MLMetricsPublisher())
    registry.register_collector(TrainingStatusCollector())
    registry.register_overview_enricher(MLOverviewEnricher())
    register_training_curve_hook(TrainingCurvePublisher().publish_hook)
