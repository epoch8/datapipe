from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Optional

from datapipe_app.observability.db import ObservabilityStore, PipelineRunRow
from datapipe_app.observability.registry import ObservabilityRegistry


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
    if run.status == "failed":
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


def build_chart_specs(
    store: ObservabilityStore,
    pipeline_id: str,
    *,
    model_id: Optional[str] = None,
) -> list[dict[str, Any]]:
    rows = store.list_metrics(pipeline_id, model_id=model_id)
    if not rows:
        return []

    by_metric: dict[str, list] = defaultdict(list)
    for row in rows:
        by_metric[row.metric_name].append(row)

    charts = []
    for metric_name, metric_rows in sorted(by_metric.items()):
        series_data = [
            {"x": _iso(r.computed_at), "y": r.metric_value, "model_id": r.model_id}
            for r in metric_rows
        ]
        charts.append(
            {
                "chart_id": f"{pipeline_id}:{metric_name}",
                "title": metric_name,
                "chart_type": "line",
                "x": {"field": "computed_at", "label": "Time"},
                "y": {"label": metric_name},
                "series": [
                    {
                        "key": metric_name,
                        "label": metric_name,
                        "data": series_data,
                    }
                ],
            }
        )
    return charts


def build_training_curves(
    store: ObservabilityStore,
    training_run_key: str,
    *,
    limit_epochs: Optional[int] = None,
) -> list[dict[str, Any]]:
    rows = store.list_training_epoch_metrics(training_run_key, limit_epochs=limit_epochs)
    if not rows:
        return []

    loss_metrics = {"train_box_loss", "train_cls_loss", "train_dfl_loss", "val_box_loss", "val_cls_loss", "loss", "val_loss"}
    map_metrics = {"metrics_mAP_0_5", "metrics_mAP_0_5_0_95", "val_f1_score", "metrics_mAP50"}

    groups: dict[str, list] = {"losses": [], "map": [], "other": []}
    by_name: dict[str, list] = {}
    for row in rows:
        by_name.setdefault(row.metric_name, []).append(row)

    charts = []
    loss_series = []
    map_series = []
    for name, name_rows in sorted(by_name.items()):
        data = [{"x": r.epoch, "y": r.metric_value} for r in sorted(name_rows, key=lambda x: x.epoch)]
        series = {"key": name, "label": name, "data": data}
        if name in loss_metrics or "loss" in name.lower():
            loss_series.append(series)
        elif name in map_metrics or "map" in name.lower() or name.startswith("val_"):
            map_series.append(series)
        else:
            charts.append(
                {
                    "chart_id": f"{training_run_key}:{name}",
                    "title": name,
                    "chart_type": "line",
                    "x": {"field": "epoch", "label": "Epoch"},
                    "y": {"label": name},
                    "series": [series],
                }
            )

    if loss_series:
        charts.insert(
            0,
            {
                "chart_id": f"{training_run_key}:losses",
                "title": "Losses",
                "chart_type": "line",
                "x": {"field": "epoch", "label": "Epoch"},
                "y": {"label": "Loss"},
                "series": loss_series,
            },
        )
    if map_series:
        charts.insert(
            1 if loss_series else 0,
            {
                "chart_id": f"{training_run_key}:map",
                "title": "Validation metrics",
                "chart_type": "line",
                "x": {"field": "epoch", "label": "Epoch"},
                "y": {"label": "Metric"},
                "series": map_series,
            },
        )
    return charts
