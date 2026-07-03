from __future__ import annotations

from typing import Any, Optional

from datapipe.compute import Catalog
from datapipe.datatable import DataStore

from datapipe_ml.observability.runs_catalog import TrainingRunCatalog
from datapipe_ml.observability.training import TrainingStatusCollector


def _latest_f1_metric(store: Any, pipeline_id: str) -> Optional[dict[str, Any]]:
    rows = store.list_metrics(pipeline_id)
    if not rows:
        return None
    f1_rows = [row for row in rows if row.metric_name == "f1_score"]
    if not f1_rows:
        return None
    latest = f1_rows[-1]
    return {
        "model_id": latest.model_id,
        "metric_value": latest.metric_value,
        "computed_at": latest.computed_at.isoformat() if latest.computed_at else None,
    }


def _latest_epoch_progress(store: Any, run_key: str) -> dict[str, Any]:
    rows = store.list_training_epoch_metrics(run_key)
    if not rows:
        return {}
    latest_epoch = max(row.epoch for row in rows)
    total_epochs = next((row.total_epochs for row in rows if row.total_epochs), None)
    return {"epoch": latest_epoch, "total_epochs": total_epochs}


class MLOverviewEnricher:
    def __init__(self) -> None:
        self._catalog = TrainingRunCatalog()

    def enrich_overview_card(
        self,
        *,
        pipeline_id: str,
        ds: DataStore | None,
        catalog: Catalog | None,
        store: Any,
    ) -> dict[str, Any] | None:
        rows = TrainingStatusCollector().collect_pipeline_status(
            pipeline_id=pipeline_id,
            ds=ds,
            catalog=catalog,
        )
        active = [row for row in rows if row["status"] == "running" and not row.get("lease_expired")]
        completed = [row for row in rows if row["status"] == "completed"]
        latest_f1 = _latest_f1_metric(store, pipeline_id)

        if active:
            run = active[-1]
            progress = _latest_epoch_progress(store, run["run_key"])
            payload: dict[str, Any] = {
                "active": True,
                "active_count": len(active),
                "run_key": run["run_key"],
                "model_id": run.get("model_id"),
                "status": run["status"],
                "heartbeat_age_s": run.get("heartbeat_age_s"),
            }
            payload.update(progress)
            return {"type": "ml_training", "payload": payload}

        if not rows and latest_f1 is None:
            return None

        payload = {
            "active": False,
            "active_count": 0,
            "completed_count": len(completed),
            "latest_completed_model_id": completed[-1].get("model_id") if completed else None,
            "latest_f1": latest_f1,
        }
        return {"type": "ml_training", "payload": payload}

    def enrich_pipeline_detail(
        self,
        *,
        pipeline_id: str,
        ds: DataStore | None,
        catalog: Catalog | None,
        store: Any,
    ) -> list[dict[str, Any]]:
        enrichments: list[dict[str, Any]] = []
        runs = self._catalog.list_runs(
            pipeline_id=pipeline_id,
            ds=ds,
            catalog=catalog,
            store=store,
        )
        if runs:
            enrichments.append({"type": "ml_training_runs", "payload": {"rows": runs}})

        latest_f1 = _latest_f1_metric(store, pipeline_id)
        if latest_f1:
            enrichments.append({"type": "ml_metrics_summary", "payload": latest_f1})

        active = [row for row in runs if row.get("status") == "running" and not row.get("lease_expired")]
        if active:
            detail = self._catalog.build_run_detail(
                run_key=active[-1]["run_key"],
                pipeline_id=pipeline_id,
                ds=ds,
                catalog=catalog,
                store=store,
            )
            if detail:
                enrichments.append({"type": "ml_training_detail", "payload": detail})

        return enrichments
