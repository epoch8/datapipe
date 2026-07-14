from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional

from datapipe_app.observability.store.db import ObservabilityStore, PipelineRunRow
from datapipe_app.observability.plugins.registry import ObservabilityRegistry


def _iso(dt: Optional[datetime]) -> Optional[str]:
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.isoformat()


def _relative_age(dt: Optional[datetime]) -> Optional[str]:
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    delta = datetime.now(timezone.utc) - dt
    seconds = int(delta.total_seconds())
    if seconds < 60:
        return f"{seconds}s ago"
    if seconds < 3600:
        return f"{seconds // 60} min ago"
    if seconds < 86400:
        return f"{seconds // 3600}h ago"
    return f"{seconds // 86400}d ago"


def _health_from_run(
    run: Optional[PipelineRunRow],
    *,
    training_preview: Optional[dict[str, Any]] = None,
) -> str:
    if training_preview and training_preview.get("status") == "running":
        return "training"
    if run is None:
        return "healthy"
    if run.status == "running":
        return "running"
    if run.status in ("failed", "interrupted"):
        return "failed"
    return "healthy"


def _primary_action(
    health: str,
    pipeline_id: str,
    last_run_id: Optional[str],
    training_preview: Optional[dict[str, Any]],
) -> dict[str, str]:
    if health == "training" and training_preview:
        return {"type": "view_training", "target": f"/pipelines/{pipeline_id}"}
    if health == "failed" and last_run_id:
        return {"type": "view_run", "target": f"/runs/{last_run_id}"}
    return {"type": "view_details", "target": f"/pipelines/{pipeline_id}"}


def build_overview(
    store: ObservabilityStore,
    registry: ObservabilityRegistry,
    *,
    ds=None,
    catalog=None,
) -> dict[str, Any]:
    cards = []
    for pipeline in store.list_pipelines():
        last_run = store.get_last_run(pipeline.pipeline_id)
        schedule = store.get_schedule(pipeline.pipeline_id)

        training_preview = None
        enrichments: list[dict[str, Any]] = []
        for enricher in registry.enrichers:
            try:
                extra = enricher.enrich_overview_card(
                    pipeline_id=pipeline.pipeline_id,
                    ds=ds,
                    catalog=catalog,
                    store=store,
                )
                if extra:
                    enrichments.append(extra)
                    if extra.get("type") == "ml_training" and extra.get("payload", {}).get("active"):
                        training_preview = extra["payload"]
            except Exception:
                pass

        health = _health_from_run(last_run, training_preview=training_preview)
        cards.append(
            {
                "pipeline_id": pipeline.pipeline_id,
                "display_name": pipeline.display_name,
                "task_type": pipeline.task_type,
                "health": health,
                "last_run_at": _iso(last_run.started_at if last_run else None),
                "last_run_age": _relative_age(last_run.started_at if last_run else None),
                "last_run_id": last_run.run_id if last_run else None,
                "last_run_status": last_run.status if last_run else None,
                "next_run_at": _iso(schedule.next_run_at if schedule else None),
                "error_snippet": (last_run.error[:120] if last_run and last_run.error else None),
                "training_preview": training_preview,
                "primary_action": _primary_action(
                    health,
                    pipeline.pipeline_id,
                    last_run.run_id if last_run else None,
                    training_preview,
                ),
                "enrichments": enrichments,
            }
        )
    cards.sort(key=lambda c: (c["health"] != "failed", c["display_name"]))
    return {"pipelines": cards, "count": len(cards)}
