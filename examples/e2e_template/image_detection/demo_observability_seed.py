#!/usr/bin/env python3
"""
Demo observability seed for image_detection_e2e pipeline.

Generates realistic metrics runs, per-class metrics, and training curve snapshots
into the observability DB and analytics cache tables.

DO NOT run automatically — intended for manual demo setup:

    python demo_observability_seed.py --pipeline-id image_detection_e2e --observability-db sqlite:///./obs.db

Requires: datapipe-app, datapipe-ml[observability]
"""

from __future__ import annotations

import argparse
import hashlib
import random
from datetime import datetime, timedelta, timezone

from datapipe_app.observability.analytics_views import refresh_analytics_views
from datapipe_app.observability.db import ObservabilityStore


CLASSES = [
    "cat", "dog", "person", "car", "bicycle", "bird", "horse", "sheep",
    "cow", "elephant", "bear", "zebra", "giraffe", "backpack", "umbrella",
]

MODELS = ["yolo11x.pt", "yolov8l", "yolov8m", "yolov8n"]
SUBSETS = ["train", "val", "test"]


def _run_id(model_id: str, subset: str, day: datetime) -> str:
    raw = f"{model_id}|{subset}|{day.date()}"
    return hashlib.md5(raw.encode()).hexdigest()[:12]


def seed_metrics(store: ObservabilityStore, pipeline_id: str, *, days: int = 30, runs_per_day: int = 2) -> None:
    rng = random.Random(42)
    now = datetime.now(timezone.utc)
    for d in range(days):
        day = now - timedelta(days=d)
        for i in range(runs_per_day):
            model_id = MODELS[(d + i) % len(MODELS)]
            subset = SUBSETS[(d + i) % len(SUBSETS)]
            base = 0.75 + (days - d) * 0.002 + rng.uniform(-0.02, 0.02)
            if d == 3 and subset == "test":
                base -= 0.015  # intentional recall regression
            metrics = {
                "mAP50": min(0.95, base + 0.08),
                "mAP50_95": min(0.75, base * 0.65),
                "precision": min(0.95, base + 0.05),
                "recall": min(0.95, base + rng.uniform(-0.03, 0.02)),
                "f1_score": min(0.95, base + 0.03),
                "iou_mean": min(0.9, base - 0.05),
                "images_support": 15000 + rng.randint(0, 5000),
                "support": 8000 + rng.randint(0, 3000),
            }
            computed_at = day.replace(hour=9 + i * 4, minute=rng.randint(0, 59))
            for name, value in metrics.items():
                store.upsert_pipeline_metric(
                    {
                        "pipeline_id": pipeline_id,
                        "model_id": model_id,
                        "subset_id": subset,
                        "metric_name": name,
                        "metric_value": float(value),
                        "computed_at": computed_at,
                        "source_table": "demo_seed",
                        "task_type": "detection",
                    }
                )


def seed_training_curves(store: ObservabilityStore, pipeline_id: str, *, run_keys: list[str] | None = None) -> None:
    rng = random.Random(99)
    keys = run_keys or ["240701-0921", "240630-0810", "240628-1542"]
    metric_names = [
        "train_box_loss", "train_cls_loss", "val_box_loss", "val_cls_loss",
        "metrics_mAP_0_5", "metrics_mAP_0_5_to_0_95", "lr_pg0",
    ]
    for ki, run_key in enumerate(keys):
        for epoch in range(50):
            for metric in metric_names:
                if "loss" in metric:
                    val = max(0.05, 2.0 - epoch * 0.035 + ki * 0.1 + rng.uniform(-0.05, 0.05))
                elif "mAP" in metric:
                    val = min(0.95, 0.3 + epoch * 0.012 + ki * 0.02)
                else:
                    val = 0.01 * (0.9 ** epoch)
                store.upsert_training_epoch_metric(
                    {
                        "pipeline_id": pipeline_id,
                        "training_run_key": run_key,
                        "epoch": epoch,
                        "total_epochs": 50,
                        "metric_name": metric,
                        "metric_value": float(val),
                        "recorded_at": datetime.now(timezone.utc),
                    }
                )


def main() -> None:
    parser = argparse.ArgumentParser(description="Seed demo observability data")
    parser.add_argument("--pipeline-id", default="image_detection_e2e")
    parser.add_argument("--observability-db", required=True, help="SQLAlchemy DB URL for observability store")
    parser.add_argument("--days", type=int, default=30)
    args = parser.parse_args()

    store = ObservabilityStore.from_url(args.observability_db)
    store.register_pipeline(args.pipeline_id, display_name="Image Detection E2E", task_type="detection")

    print(f"Seeding metrics for {args.pipeline_id}…")
    seed_metrics(store, args.pipeline_id, days=args.days)
    print("Seeding training curves…")
    seed_training_curves(store, args.pipeline_id)
    print("Refreshing analytics views…")
    refresh_analytics_views(store.engine, pipeline_id=args.pipeline_id, store=store)
    print("Done. Open Datapipe Ops → Metrics / Training / SQL Studio.")


if __name__ == "__main__":
    main()
