from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

import fsspec
from datapipe.store.database import DBConn
from pathy import Pathy

# --- object storage (MinIO / S3) ------------------------------------------------
AWS_KEY = os.environ.get("AWS_ACCESS_KEY_ID", "")
AWS_SECRET = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
S3_ENDPOINT_URL = os.environ.get("S3_ENDPOINT_URL")
S3_PUBLIC_URL = os.environ.get("S3_PUBLIC_URL", S3_ENDPOINT_URL or "http://localhost:9000")

# --- classes (lowercase, as in COCO) -------------------------------------------
# detection classes for this demo; keep lowercase to match COCO category names so
# injected ground truth and model predictions use identical label strings.
DETECTION_CLASSES = ["cat", "dog"]
COCO_CAT_IDS = {17: "cat", 18: "dog"}  # COCO category_id -> class name

# --- tags -----------------------------------------------------------------------
# tag_id is the human-readable tag NAME itself (text). Two columns only: (tag_id, tag_description).
# No numeric surrogate / separate tag_name — the UI shows the tag by name directly.

# --- single storage root --------------------------------------------------------
# Input images live under <root>/images; the pipeline working_dir under <root>/datapipe
# (siblings) so listing input images never re-ingests training artifacts.
DATAPIPE_DIR_ROOT = os.environ.get("DATAPIPE_TAGS_DIR", "s3://datapipe-tags").rstrip("/")
INPUT_IMAGES_DIR = f"{DATAPIPE_DIR_ROOT}/images"


def _is_cloud_path(path: str) -> bool:
    protocol, _ = fsspec.core.split_protocol(path)
    return protocol not in (None, "file")


def datapipe_working_dir() -> str:
    if _is_cloud_path(DATAPIPE_DIR_ROOT):
        return f"{DATAPIPE_DIR_ROOT}/datapipe"
    local = Path(DATAPIPE_DIR_ROOT).resolve() / "datapipe"
    local.mkdir(parents=True, exist_ok=True)
    return str(local)


DATAPIPE_DIR = datapipe_working_dir()


def input_storage_options() -> dict:
    if not _is_cloud_path(INPUT_IMAGES_DIR):
        return {}
    client_kwargs: dict = {"region_name": AWS_REGION}
    if S3_ENDPOINT_URL:
        client_kwargs["endpoint_url"] = S3_ENDPOINT_URL
    return {"key": AWS_KEY, "secret": AWS_SECRET, "client_kwargs": client_kwargs}


def input_bucket() -> Optional[str]:
    if not _is_cloud_path(DATAPIPE_DIR_ROOT):
        return None
    return Pathy(DATAPIPE_DIR_ROOT).root


def input_image_url(rel_key: str) -> str:
    """Browser- and fsspec-readable URL for an input image relative to INPUT_IMAGES_DIR."""
    if _is_cloud_path(INPUT_IMAGES_DIR):
        target = Pathy(INPUT_IMAGES_DIR) / rel_key
        return f"{S3_PUBLIC_URL.rstrip('/')}/{target.root}/{target.key}"
    return (Path(INPUT_IMAGES_DIR) / rel_key).resolve().as_uri()


def datapipe_tmp_folder() -> str:
    if _is_cloud_path(DATAPIPE_DIR):
        return str(Path(os.environ.get("DATAPIPE_TAGS_TMP_DIR", "/tmp/datapipe-tags")).resolve())
    return str(Path(DATAPIPE_DIR) / "tmp")


# --- database -------------------------------------------------------------------
DB_URL = os.environ.get("DB_URL")
if not DB_URL:
    raise RuntimeError(
        "DB_URL is required. Copy examples/detection_tags/.env.example to .env, "
        "start docker compose, and run: set -a && source .env && set +a"
    )

DB_SCHEMA = os.environ.get("DB_SCHEMA", "public")
DBCONN = DBConn(DB_URL, DB_SCHEMA)

CLICKHOUSE_RUN_LOGS_URL = os.environ.get("CLICKHOUSE_RUN_LOGS_URL")
if not CLICKHOUSE_RUN_LOGS_URL:
    raise RuntimeError(
        "CLICKHOUSE_RUN_LOGS_URL is required. Copy examples/detection_tags/.env.example to .env, "
        "start docker compose (clickhouse service), and run: set -a && source .env && set +a"
    )

# --- FiftyOne -------------------------------------------------------------------
# FiftyOne stores dataset metadata in MongoDB (FIFTYONE_DATABASE_URI, brought up by
# docker compose) and renders samples from LOCAL image files, so the fiftyone stage
# downloads S3 images to LOCAL_IMAGES_DIR first.
def _local_images_dir() -> Path:
    explicit = os.environ.get("LOCAL_IMAGES_DIR")
    if explicit:
        return Path(explicit).resolve()
    if _is_cloud_path(DATAPIPE_DIR_ROOT):
        return Path(os.environ.get("DATAPIPE_TAGS_TMP_DIR", "/tmp/datapipe-tags")).resolve() / "local_images"
    return Path(DATAPIPE_DIR).resolve().parent / "local_images"


FIFTYONE_DATASET_NAME = os.environ.get("FIFTYONE_DATASET_NAME", "datapipe_detection_tags")
LOCAL_IMAGES_DIR = _local_images_dir()

from datapipe.executor import ExecutorConfig

_MAX_CPU = os.cpu_count() or 4


def _use_gpu() -> bool:
    explicit = os.environ.get("DATAPIPE_USE_GPU")
    if explicit is not None:
        return explicit.strip().lower() not in {"0", "false", "no", "off"}
    try:
        import torch

        return torch.cuda.is_available()
    except Exception:
        return False


def parallel_io_executor(*, parallelism_cap: int = 16, cpu_per_task: float = 0.2) -> ExecutorConfig:
    """Embarrassingly parallel I/O and image prep (resize, download, upload)."""
    return ExecutorConfig(cpu=cpu_per_task, parallelism=min(_MAX_CPU, parallelism_cap))


def inference_executor(*, parallelism: int | None = None) -> ExecutorConfig:
    """GPU inference when CUDA is available; CPU fallback otherwise."""
    if _use_gpu():
        if parallelism is None:
            try:
                import torch

                parallelism = max(1, torch.cuda.device_count())
            except Exception:
                parallelism = 1
        return ExecutorConfig(gpu=1, cpu=1.0, parallelism=parallelism)
    return ExecutorConfig(cpu=1.0, parallelism=min(_MAX_CPU, 4))


def metrics_executor(*, parallelism_cap: int = 8, cpu_per_task: float = 0.5) -> ExecutorConfig:
    """CPU-bound metrics aggregation over image batches."""
    return ExecutorConfig(cpu=cpu_per_task, parallelism=min(_MAX_CPU, parallelism_cap))
