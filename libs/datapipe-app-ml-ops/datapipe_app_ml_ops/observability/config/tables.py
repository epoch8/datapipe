from __future__ import annotations

from dataclasses import asdict, dataclass

from datapipe_app.observability.config.tables import validate_observability_table_names


@dataclass(frozen=True)
class MLObservabilityTableConfig:
    metrics_candidates: str = "metrics_candidates"

    def __post_init__(self) -> None:
        validate_observability_table_names(self)

    def table_names(self) -> dict[str, str]:
        return asdict(self)
