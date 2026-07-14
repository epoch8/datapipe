from __future__ import annotations

from dataclasses import asdict, dataclass

from datapipe_app.observability.tables import validate_observability_table_names


@dataclass(frozen=True)
class MLObservabilityTableConfig:
    metrics_candidates: str = "metrics_candidates"
    analytics_metrics_on_subset: str = "analytics_metrics_on_subset"
    analytics_metrics_by_class: str = "analytics_metrics_by_class"
    analytics_training_runs: str = "analytics_training_runs"

    def __post_init__(self) -> None:
        validate_observability_table_names(self)

    def table_names(self) -> dict[str, str]:
        return asdict(self)
