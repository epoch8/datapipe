from __future__ import annotations

import os
from pathlib import Path

import torch
from datapipe.store.database import DBConn

LOCAL_IMAGES_DIR = Path(os.environ.get("LOCAL_IMAGES_DIR", "./data/images")).resolve()
LABELS_JSON = os.environ.get("LABELS_JSON")
EMBEDDINGS_DIR = Path(os.environ.get("EMBEDDINGS_DIR", "./data/embeddings")).resolve()

EMBEDDERS = [
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
