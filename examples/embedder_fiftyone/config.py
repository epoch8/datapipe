from __future__ import annotations

import os
from pathlib import Path

import torch
from datapipe.store.database import DBConn

_local_images_dir_raw = os.environ.get("LOCAL_IMAGES_DIR")
LOCAL_IMAGES_DIR = Path(_local_images_dir_raw).resolve() if _local_images_dir_raw else None
LABELS_JSON = os.environ.get("LABELS_JSON")
EMBEDDINGS_DIR = Path(os.environ.get("EMBEDDINGS_DIR", "./data/embeddings")).resolve()

ZOO_DATASET = os.environ.get("ZOO_DATASET", "caltech101")
ZOO_LABEL_FIELD = os.environ.get("ZOO_LABEL_FIELD", "ground_truth")
ZOO_NUM_CLASSES = int(os.environ.get("ZOO_NUM_CLASSES", "10"))

EMBEDDERS = [
    {
        "embedder_id": "dinov2_base",
        "cls": "HFDinoModel",
        "init_kwargs": {"ckpt": "facebook/dinov2-base"},
        "batch_size": 32,
        "max_size": 512,
    },
    {
        "embedder_id": "dinov2_large",
        "cls": "HFDinoModel",
        "init_kwargs": {"ckpt": "facebook/dinov2-large"},
        "batch_size": 8,
        "max_size": 512,
    },
    {
        "embedder_id": "dinov3_vitl16",
        "cls": "HFDinoModel",
        "init_kwargs": {"ckpt": "facebook/dinov3-vitl16-pretrain-lvd1689m"},
        "batch_size": 8,
        "max_size": 512,
    },
]

BRAIN_METHODS = ["umap"]
DEVICE = "cuda:0" if torch.cuda.is_available() else "cpu"
IMAGE_SUFFIXES = {".jpg", ".jpeg", ".png"}

DB_URL = os.environ.get("DB_URL")
DBCONN = DBConn(DB_URL, "public")

FIFTYONE_DATASET_NAME = os.environ.get("FIFTYONE_DATASET_NAME", "datapipe_embedder_fiftyone")


def use_local_images() -> bool:
    if LOCAL_IMAGES_DIR is None or not LOCAL_IMAGES_DIR.exists():
        return False
    return any(
        path.is_file() and path.suffix.lower() in IMAGE_SUFFIXES for path in LOCAL_IMAGES_DIR.rglob("*")
    )
