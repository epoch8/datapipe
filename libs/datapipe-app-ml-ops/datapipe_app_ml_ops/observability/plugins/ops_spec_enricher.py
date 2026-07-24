from __future__ import annotations

from typing import Any

from datapipe.compute import Catalog
from datapipe.datatable import DataStore

from datapipe_app.observability.store.db import ObservabilityStore
from datapipe_app_ml_ops.ops.spec_registry import OpsSpecRegistry
from datapipe_app_ml_ops.ops.ops_spec_metrics import latest_eval_metric_from_specs


class OpsSpecOverviewEnricher:
    def __init__(self, ops_specs: OpsSpecRegistry) -> None:
        self.ops_specs = ops_specs

    def enrich_pipeline_detail(
        self,
        *,
        pipeline_id: str,
        ds: DataStore | None,
        catalog: Catalog | None,
        store: ObservabilityStore,
    ) -> list[dict[str, Any]]:
        if ds is None or catalog is None or not self.ops_specs.list():
            return []
        latest = latest_eval_metric_from_specs(self.ops_specs, ds, catalog)
        if latest is None:
            return []
        return [{"type": "ops_metrics_summary", "payload": latest}]
