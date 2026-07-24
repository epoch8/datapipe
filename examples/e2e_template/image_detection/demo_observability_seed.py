#!/usr/bin/env python3
"""
Register a demo pipeline in the observability DB.

Metrics and training status are read from the pipeline catalog at runtime;
this script only registers the pipeline row for Ops UI bootstrap.

    python demo_observability_seed.py --pipeline-id image_detection_e2e --observability-db sqlite:///./obs.db
"""

from __future__ import annotations

import argparse

from datapipe_app.observability.store.db import ObservabilityStore


def main() -> None:
    parser = argparse.ArgumentParser(description="Register demo pipeline in observability store")
    parser.add_argument("--pipeline-id", default="image_detection_e2e")
    parser.add_argument("--observability-db", required=True, help="SQLAlchemy DB URL for observability store")
    args = parser.parse_args()

    store = ObservabilityStore.from_url(args.observability_db)
    store.register_pipeline(args.pipeline_id, display_name="Image Detection E2E", task_type="detection")
    print(f"Registered pipeline {args.pipeline_id}. Metrics come from pipeline catalog tables.")


if __name__ == "__main__":
    main()
