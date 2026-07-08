#!/usr/bin/env python3
"""Seed Ops UI smoke data for the detection_tags pipeline.

Run from examples/detection_tags:

    set -a && source .env && set +a
    uv run python scripts/seed_ops_smoke.py
"""
from __future__ import annotations

import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
DETECTION_DIR = ROOT / "detection"
sys.path.insert(0, str(DETECTION_DIR))

if not os.environ.get("DB_URL"):
    env_example = ROOT / ".env.example"
    if env_example.exists():
        for raw in env_example.read_text().splitlines():
            line = raw.strip()
            if not line.startswith("export ") or "=" not in line:
                continue
            key, value = line[len("export ") :].split("=", 1)
            value = os.path.expandvars(value.strip().strip('"'))
            os.environ.setdefault(key, value)

import config  # noqa: E402
from app import catalog, ds  # noqa: E402

CLASS_NAMES = ["cat", "dog"]

FROZEN_DATASETS = [
    {
        "detection_frozen_dataset_id": "fd_20250701_1430_baseline",
        "detection_frozen_dataset__created_at": datetime(2025, 7, 1, 14, 30),
        "detection_frozen_dataset__folder_filepath": (
            f"{config.DATAPIPE_DIR}/detection_frozen_dataset/fd_20250701_1430_baseline"
        ),
        "detection_frozen_dataset__images_count": 100,
        "detection_frozen_dataset__train_images_count": 80,
        "detection_frozen_dataset__val_images_count": 20,
        "detection_frozen_dataset__test_images_count": 0,
        "detection_frozen_dataset__class_names": CLASS_NAMES,
    },
    {
        "detection_frozen_dataset_id": "fd_20250705_0915_night",
        "detection_frozen_dataset__created_at": datetime(2025, 7, 5, 9, 15),
        "detection_frozen_dataset__folder_filepath": (
            f"{config.DATAPIPE_DIR}/detection_frozen_dataset/fd_20250705_0915_night"
        ),
        "detection_frozen_dataset__images_count": 60,
        "detection_frozen_dataset__train_images_count": 48,
        "detection_frozen_dataset__val_images_count": 12,
        "detection_frozen_dataset__test_images_count": 0,
        "detection_frozen_dataset__class_names": CLASS_NAMES,
    },
    {
        "detection_frozen_dataset_id": "fd_20250708_1610_augmented",
        "detection_frozen_dataset__created_at": datetime(2025, 7, 8, 16, 10),
        "detection_frozen_dataset__folder_filepath": (
            f"{config.DATAPIPE_DIR}/detection_frozen_dataset/fd_20250708_1610_augmented"
        ),
        "detection_frozen_dataset__images_count": 80,
        "detection_frozen_dataset__train_images_count": 64,
        "detection_frozen_dataset__val_images_count": 16,
        "detection_frozen_dataset__test_images_count": 0,
        "detection_frozen_dataset__class_names": CLASS_NAMES,
    },
]

MODELS = [
    {
        "detection_model_id": "yolov8n_tags_baseline_v1",
        "detection_frozen_dataset_id": "fd_20250701_1430_baseline",
        "detection_train_config_id": "cfg_yolov8n_320_baseline",
        "training_status_id": "ts_baseline_v1",
        "training_status__run_key": "250701-1430",
        "detection_model__model_path": (
            f"{config.DATAPIPE_DIR}/models/detection_frozen_dataset/"
            "fd_20250701_1430_baseline/yolov8n_tags_baseline_v1/weights/best.pt"
        ),
        "is_best": False,
    },
    {
        "detection_model_id": "yolov8n_tags_night_v2",
        "detection_frozen_dataset_id": "fd_20250705_0915_night",
        "detection_train_config_id": "cfg_yolov8n_320_night",
        "training_status_id": "ts_night_v2",
        "training_status__run_key": "250705-0915",
        "detection_model__model_path": (
            f"{config.DATAPIPE_DIR}/models/detection_frozen_dataset/"
            "fd_20250705_0915_night/yolov8n_tags_night_v2/weights/best.pt"
        ),
        "is_best": True,
    },
    {
        "detection_model_id": "yolov8n_tags_augmented_v3",
        "detection_frozen_dataset_id": "fd_20250708_1610_augmented",
        "detection_train_config_id": "cfg_yolov8n_320_augmented",
        "training_status_id": "ts_augmented_v3",
        "training_status__run_key": "250708-1610",
        "detection_model__model_path": (
            f"{config.DATAPIPE_DIR}/models/detection_frozen_dataset/"
            "fd_20250708_1610_augmented/yolov8n_tags_augmented_v3/weights/best.pt"
        ),
        "is_best": False,
    },
]

# val weighted F1 drives FindBestModel; night model wins.
OVERALL_METRICS = {
    "yolov8n_tags_baseline_v1": {
        "train": dict(images=80, support=530, accuracy=0.79, wp=0.82, wr=0.79, wf1=0.80, mp=0.81, mr=0.78, mf1=0.79),
        "val": dict(images=20, support=122, accuracy=0.72, wp=0.76, wr=0.72, wf1=0.74, mp=0.75, mr=0.71, mf1=0.73),
        "test": dict(images=0, support=0, accuracy=0.0, wp=0.0, wr=0.0, wf1=0.0, mp=0.0, mr=0.0, mf1=0.0),
    },
    "yolov8n_tags_night_v2": {
        "train": dict(images=48, support=312, accuracy=0.83, wp=0.84, wr=0.83, wf1=0.84, mp=0.85, mr=0.82, mf1=0.83),
        "val": dict(images=12, support=78, accuracy=0.79, wp=0.84, wr=0.79, wf1=0.82, mp=0.83, mr=0.80, mf1=0.81),
        "test": dict(images=0, support=0, accuracy=0.0, wp=0.0, wr=0.0, wf1=0.0, mp=0.0, mr=0.0, mf1=0.0),
    },
    "yolov8n_tags_augmented_v3": {
        "train": dict(images=64, support=428, accuracy=0.82, wp=0.83, wr=0.82, wf1=0.83, mp=0.84, mr=0.81, mf1=0.82),
        "val": dict(images=16, support=106, accuracy=0.79, wp=0.81, wr=0.79, wf1=0.80, mp=0.80, mr=0.78, mf1=0.79),
        "test": dict(images=0, support=0, accuracy=0.0, wp=0.0, wr=0.0, wf1=0.0, mp=0.0, mr=0.0, mf1=0.0),
    },
}

# Same tag batch_night evaluated by every model (baseline struggles, night model best).
TAG_OVERALL_METRICS = {
    "yolov8n_tags_baseline_v1": dict(
        images=8, support=46, accuracy=0.65, wp=0.68, wr=0.63, wf1=0.65, mp=0.67, mr=0.62, mf1=0.64,
    ),
    "yolov8n_tags_night_v2": dict(
        images=8, support=46, accuracy=0.83, wp=0.86, wr=0.83, wf1=0.84, mp=0.85, mr=0.82, mf1=0.84,
    ),
    "yolov8n_tags_augmented_v3": dict(
        images=8, support=46, accuracy=0.74, wp=0.78, wr=0.74, wf1=0.76, mp=0.77, mr=0.73, mf1=0.75,
    ),
}

CLASS_METRICS = [
    ("yolov8n_tags_baseline_v1", "val", "cat", 28, 9, 11, 12),
    ("yolov8n_tags_baseline_v1", "val", "dog", 24, 8, 10, 10),
    ("yolov8n_tags_night_v2", "val", "cat", 34, 4, 5, 14),
    ("yolov8n_tags_night_v2", "val", "dog", 30, 5, 6, 12),
    ("yolov8n_tags_augmented_v3", "val", "cat", 31, 6, 7, 13),
    ("yolov8n_tags_augmented_v3", "val", "dog", 27, 7, 8, 11),
]

# batch_night / val — per-class breakdown for each model on the same tagged slice.
TAG_CLASS_METRICS = [
    ("yolov8n_tags_baseline_v1", "batch_night", "val", "cat", 14, 6, 8, 5),
    ("yolov8n_tags_baseline_v1", "batch_night", "val", "dog", 12, 5, 7, 4),
    ("yolov8n_tags_night_v2", "batch_night", "val", "cat", 22, 2, 3, 8),
    ("yolov8n_tags_night_v2", "batch_night", "val", "dog", 20, 2, 2, 7),
    ("yolov8n_tags_augmented_v3", "batch_night", "val", "cat", 18, 4, 5, 6),
    ("yolov8n_tags_augmented_v3", "batch_night", "val", "dog", 16, 3, 4, 5),
]


def _prf(tp: int, fp: int, fn: int) -> tuple[float, float, float]:
    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall = tp / (tp + fn) if (tp + fn) else 0.0
    f1 = (2 * precision * recall / (precision + recall)) if (precision + recall) else 0.0
    return round(precision, 4), round(recall, 4), round(f1, 4)


def _overall_row(model_id: str, subset_id: str, m: dict) -> dict:
    row = {
        "detection_model_id": model_id,
        "subset_id": subset_id,
        "calc__images_support": m["images"],
        "calc__support": m["support"],
        "calc__accuracy": round(m["accuracy"], 4),
        "calc__weighted_precision": round(m["wp"], 4),
        "calc__weighted_recall": round(m["wr"], 4),
        "calc__weighted_f1_score": round(m["wf1"], 4),
        "calc__macro_precision": round(m["mp"], 4),
        "calc__macro_recall": round(m["mr"], 4),
        "calc__macro_f1_score": round(m["mf1"], 4),
    }
    for prefix in ("weighted_without_pseudo_classes", "macro_without_pseudo_classes"):
        for metric, src in (("precision", "wp" if "weighted" in prefix else "mp"),
                            ("recall", "wr" if "weighted" in prefix else "mr"),
                            ("f1_score", "wf1" if "weighted" in prefix else "mf1")):
            row[f"calc__{prefix}_{metric}"] = round(m[src], 4)
    return row


def _tag_overall_row(model_id: str, tag_id: str, subset_id: str, m: dict) -> dict:
    row = _overall_row(model_id, subset_id, m)
    row["tag_id"] = tag_id
    return row


def _class_row(model_id: str, subset_id: str, label: str, tp: int, fp: int, fn: int, images: int) -> dict:
    p, r, f1 = _prf(tp, fp, fn)
    return {
        "detection_model_id": model_id,
        "subset_id": subset_id,
        "label": label,
        "calc__images_support": images,
        "calc__support": tp + fn,
        "calc__TP": tp,
        "calc__FP": fp,
        "calc__FN": fn,
        "calc__precision": p,
        "calc__recall": r,
        "calc__f1_score": f1,
    }


def _tag_class_row(
    model_id: str, tag_id: str, subset_id: str, label: str, tp: int, fp: int, fn: int, images: int,
) -> dict:
    row = _class_row(model_id, subset_id, label, tp, fp, fn, images)
    row["tag_id"] = tag_id
    return row


def _drop_schema() -> None:
    from sqlalchemy import text

    schema = config.DB_SCHEMA
    with ds.meta_dbconn.con.begin() as con:
        con.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))


def _create_all_tables() -> None:
    print("Creating tables via datapipe db create-all…")
    subprocess.run(
        ["uv", "run", "datapipe", "--pipeline", "app:app", "db", "create-all"],
        cwd=DETECTION_DIR,
        check=True,
        env=os.environ.copy(),
    )


def seed() -> None:
    _drop_schema()
    _create_all_tables()

    images = pd.DataFrame([
        {"image_name": "000000000139.jpg", "image_url": config.input_image_url("000000000139.jpg")},
        {"image_name": "000000000285.jpg", "image_url": config.input_image_url("000000000285.jpg")},
        {"image_name": "000000000632.jpg", "image_url": config.input_image_url("000000000632.jpg")},
        {"image_name": "000000000139__night.jpg", "image_url": config.input_image_url("000000000139__night.jpg")},
        {"image_name": "000000000285__night.jpg", "image_url": config.input_image_url("000000000285__night.jpg")},
        {"image_name": "000000000632__aug.jpg", "image_url": config.input_image_url("000000000632__aug.jpg")},
    ])
    ground_truth = pd.DataFrame([
        {"image_name": "000000000139.jpg", "bboxes": [[120, 80, 340, 290]], "labels": ["cat"]},
        {"image_name": "000000000285.jpg", "bboxes": [[40, 60, 220, 310]], "labels": ["dog"]},
        {"image_name": "000000000632.jpg", "bboxes": [[90, 100, 280, 350], [300, 120, 480, 360]], "labels": ["cat", "dog"]},
        {"image_name": "000000000139__night.jpg", "bboxes": [[118, 78, 338, 288]], "labels": ["cat"]},
        {"image_name": "000000000285__night.jpg", "bboxes": [[42, 58, 218, 305]], "labels": ["dog"]},
        {"image_name": "000000000632__aug.jpg", "bboxes": [[88, 98, 278, 348]], "labels": ["cat"]},
    ])
    subsets = pd.DataFrame([
        {"image_name": "000000000139.jpg", "subset_id": "train"},
        {"image_name": "000000000285.jpg", "subset_id": "train"},
        {"image_name": "000000000632.jpg", "subset_id": "val"},
        {"image_name": "000000000139__night.jpg", "subset_id": "train"},
        {"image_name": "000000000285__night.jpg", "subset_id": "val"},
        {"image_name": "000000000632__aug.jpg", "subset_id": "val"},
    ])
    tags = pd.DataFrame([
        {"tag_id": "batch_baseline", "name": "Baseline batch"},
        {"tag_id": "batch_night", "name": "Night / low-light"},
        {"tag_id": "batch_augmented", "name": "Augmented batch"},
    ])
    image_tags = pd.DataFrame([
        {"image_name": "000000000139.jpg", "tag_id": "batch_baseline"},
        {"image_name": "000000000285.jpg", "tag_id": "batch_baseline"},
        {"image_name": "000000000632.jpg", "tag_id": "batch_baseline"},
        {"image_name": "000000000139__night.jpg", "tag_id": "batch_night"},
        {"image_name": "000000000285__night.jpg", "tag_id": "batch_night"},
        {"image_name": "000000000632__aug.jpg", "tag_id": "batch_augmented"},
    ])

    metrics_on_subset = pd.DataFrame([
        _overall_row(model_id, subset_id, values)
        for model_id, by_subset in OVERALL_METRICS.items()
        for subset_id, values in by_subset.items()
    ])
    metrics_by_cls = pd.DataFrame([
        _class_row(model_id, subset_id, label, tp, fp, fn, images=12 if label == "cat" else 10)
        for model_id, subset_id, label, tp, fp, fn, _ in CLASS_METRICS
    ])
    tag_metrics = pd.DataFrame([
        _tag_overall_row(model_id, "batch_night", "val", values)
        for model_id, values in TAG_OVERALL_METRICS.items()
    ])
    tag_metrics_by_cls = pd.DataFrame([
        _tag_class_row(model_id, tag_id, subset_id, label, tp, fp, fn, images)
        for model_id, tag_id, subset_id, label, tp, fp, fn, images in TAG_CLASS_METRICS
    ])

    tables = {
        "s3_images": images,
        "image__ground_truth": ground_truth,
        "image__subset": subsets,
        "tag": tags,
        "image__tag": image_tags,
        "detection_frozen_dataset": pd.DataFrame(FROZEN_DATASETS),
        "detection_model_train": pd.DataFrame([
            {
                "detection_model_id": m["detection_model_id"],
                "detection_model__input_size": [320, 320],
                "detection_model__score_threshold": 0.25,
                "detection_model__model_path": m["detection_model__model_path"],
                "detection_model__type": "yolov8",
                "detection_model__class_names": CLASS_NAMES,
            }
            for m in MODELS
        ]),
        "detection_model_is_trained_on_detection_frozen_dataset": pd.DataFrame([
            {
                "detection_model_id": m["detection_model_id"],
                "detection_frozen_dataset_id": m["detection_frozen_dataset_id"],
                "detection_train_config_id": m["detection_train_config_id"],
                "detection_train_config__params": {"model": "yolov8n.pt", "imgsz": 320, "epochs": 10},
            }
            for m in MODELS
        ]),
        "detection_training_status": pd.DataFrame([
            {
                "training_status_id": m["training_status_id"],
                "training_status__run_key": m["training_status__run_key"],
                "training_status__launcher_type": "local",
                "training_status__launcher_config": {},
                "training_status__launcher_state": {},
                "detection_model_id": m["detection_model_id"],
                "detection_frozen_dataset_id": m["detection_frozen_dataset_id"],
                "detection_train_config_id": m["detection_train_config_id"],
                "training_status__models_dir": f"{config.DATAPIPE_DIR}/models",
                "training_status__run_dir": f"{config.DATAPIPE_DIR}/runs/{m['training_status__run_key']}",
                "training_status__status": "completed",
                "training_status__started_at": "2025-07-01T14:30:00Z",
                "training_status__finished_at": "2025-07-01T15:12:00Z",
                "training_status__attempt": 1,
                "training_status__manifest_path": (
                    f"{config.DATAPIPE_DIR}/runs/{m['training_status__run_key']}/manifest.json"
                ),
                "training_status__error": None,
                "training_status__owner_id": None,
                "training_status__heartbeat_at": None,
                "training_status__lease_expires_at": None,
            }
            for m in MODELS
        ]),
        "pipeline_model__metrics_on_subset": metrics_on_subset,
        "pipeline_model__metrics_by_cls_on_subset": metrics_by_cls,
        "pipeline_model__metrics_by_tag_on_subset": tag_metrics,
        "pipeline_model__metrics_by_tag_by_cls_on_subset": tag_metrics_by_cls,
        "attr__detection_model__is_best": pd.DataFrame([
            {"detection_model_id": m["detection_model_id"], "detection_model__is_best": m["is_best"]}
            for m in MODELS
        ]),
    }

    for name, df in tables.items():
        catalog.get_datatable(ds, name).store_chunk(df)
        print(f"  {name}: {len(df)} rows")

    print("Ops smoke data seeded.")


if __name__ == "__main__":
    print("Seeding detection_tags Ops smoke data…")
    seed()
