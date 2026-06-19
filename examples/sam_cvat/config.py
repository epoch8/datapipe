from __future__ import annotations

import os
import re
from pathlib import Path

import torch
from datapipe.store.database import DBConn

_INPUT_DIR_RAW = os.environ.get("INPUT_DIR")
INPUT_DIR = Path(_INPUT_DIR_RAW).resolve() if _INPUT_DIR_RAW else None
IMAGE_SUFFIXES = {".jpg", ".jpeg", ".png"}

HF_DATASET_NAME = os.environ.get("HF_DATASET_NAME", "HZMD/cats-n-dogs")
HF_DATASET_SPLIT = os.environ.get("HF_DATASET_SPLIT", "train")
HF_DATASET_LABEL = int(os.environ.get("HF_DATASET_LABEL", "0"))
_HF_DATASET_CACHE_FOLDER_RAW = os.environ.get("HF_DATASET_CACHE_FOLDER")
HF_DATASET_CACHE_FOLDER = (
    Path(_HF_DATASET_CACHE_FOLDER_RAW).resolve()
    if _HF_DATASET_CACHE_FOLDER_RAW
    else Path(__file__).resolve().parent / ".hf_dataset_cache"
)

DEVICE = "cuda:0" if torch.cuda.is_available() else "cpu"

HF_TOKEN = os.environ.get("HF_TOKEN", "")
SAM_TEXT_PROMPT = os.environ.get("SAM_TEXT_PROMPT", "a dog")
SAM_SCORE_THRESHOLD = float(os.environ.get("SAM_SCORE_THRESHOLD", "0.5"))
SAM_MAX_DETECTIONS = int(os.environ.get("SAM_MAX_DETECTIONS", "10"))
TASK_QUEUE_ID = os.environ.get("TASK_QUEUE_ID", "queue1")
FILES_BATCH = int(os.environ.get("FILES_BATCH", "5"))

CVAT_URL = os.environ.get("CVAT_URL", "http://localhost:8080")
CVAT_USERNAME = os.environ.get("CVAT_USERNAME", "admin")
CVAT_PASSWORD = os.environ.get("CVAT_PASSWORD", "admin")
CVAT_PROJECT_ID = int(os.environ.get("CVAT_PROJECT_ID", "1"))
CVAT_ORGANIZATION = os.environ.get("CVAT_ORGANIZATION", "")
CVAT_PROJECT_NAME = os.environ.get("CVAT_PROJECT_NAME", "datapipe-sam-cvat")

PRIMARY_KEYS = ["image_id", "task_queue_id"]

CVAT_BOX_LABEL = os.environ.get("CVAT_BOX_LABEL", "box")
CVAT_POLYGON_LABEL = os.environ.get("CVAT_POLYGON_LABEL", "mask")

DB_URL = os.environ.get("DB_URL")
DBCONN = DBConn(DB_URL, None)
