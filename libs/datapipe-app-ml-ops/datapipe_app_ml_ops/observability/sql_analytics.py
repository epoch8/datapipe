from __future__ import annotations

QUALIFIED_TABLE_MAP = {
    "datapipe_analytics.metrics_on_subset": "analytics_metrics_on_subset",
    "datapipe_analytics.metrics_by_class": "analytics_metrics_by_class",
    "datapipe_analytics.training_runs": "analytics_training_runs",
    "datapipe_analytics.artifacts": "analytics_training_runs",
    "datapipe_analytics.predictions": "analytics_metrics_on_subset",
}

BARE_TABLE_MAP = {
    "metrics_on_subset": "analytics_metrics_on_subset",
    "metrics_by_class": "analytics_metrics_by_class",
    "training_runs": "analytics_training_runs",
}
