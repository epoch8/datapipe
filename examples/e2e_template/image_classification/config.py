from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

import fsspec
from datapipe.store.database import DBConn
from pathy import Pathy

AWS_KEY = os.environ.get("AWS_ACCESS_KEY_ID", "")
AWS_SECRET = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")

S3_ENDPOINT_URL = os.environ.get("S3_ENDPOINT_URL")
S3_PUBLIC_URL = os.environ.get("S3_PUBLIC_URL", S3_ENDPOINT_URL or "http://localhost:9000")
LABEL_STUDIO_S3_ENDPOINT_URL = os.environ.get("LABEL_STUDIO_S3_ENDPOINT_URL", S3_ENDPOINT_URL)

LABEL_STUDIO_URL = os.environ.get("LABEL_STUDIO_URL", "")
LABEL_STUDIO_API_KEY = os.environ.get("LABEL_STUDIO_API_KEY", "")

PROJECT_NAME = "Datapipe classification e2e"
LABEL_CONFIG = """
<View>
  <Image name="image" value="$image_url"/>
  <Choices name="label" toName="image" choice="single" showInLine="true">
    <Choice value="Has Animal" background="#4ECDC4"/>
    <Choice value="No Animals" background="#FF6B6B"/>
  </Choices>
</View>
"""

CLASS_HAS_ANIMAL = "Has Animal"
CLASS_NO_ANIMALS = "No Animals"
CLASSIFICATION_CLASSES = [CLASS_HAS_ANIMAL, CLASS_NO_ANIMALS]

E2E_TEMPLATE_DIR = Path(__file__).resolve().parents[1]
SEED_CLASSIFICATION_LABELS_PATH = E2E_TEMPLATE_DIR / "sample_data" / "classification_labels.json"


def _is_cloud_path(path: str) -> bool:
    protocol, _ = fsspec.core.split_protocol(path)
    return protocol not in (None, "file")


# Single root for everything this example reads/writes. Local path or s3:// URL.
# Input images live under <root>/images and the pipeline working_dir under <root>/datapipe
# (siblings) so the recursive image listing in steps.py never re-ingests resized crops or
# other artifacts written under the working_dir.
DATAPIPE_E2E_DIR = os.environ.get("DATAPIPE_E2E_DIR", "s3://datapipe-e2e").rstrip("/")
INPUT_IMAGES_DIR = f"{DATAPIPE_E2E_DIR}/images"


def datapipe_working_dir() -> str:
    if _is_cloud_path(DATAPIPE_E2E_DIR):
        return f"{DATAPIPE_E2E_DIR}/datapipe"
    local = Path(DATAPIPE_E2E_DIR).resolve() / "datapipe"
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


def _input_s3_bucket() -> Optional[str]:
    if not _is_cloud_path(DATAPIPE_E2E_DIR):
        return None
    return Pathy(DATAPIPE_E2E_DIR).root


def input_image_url(rel_key: str) -> str:
    """Browser- and fsspec-readable URL for an input image relative to INPUT_IMAGES_DIR."""
    if _is_cloud_path(INPUT_IMAGES_DIR):
        target = Pathy(INPUT_IMAGES_DIR) / rel_key
        return f"{S3_PUBLIC_URL.rstrip('/')}/{target.root}/{target.key}"
    return (Path(INPUT_IMAGES_DIR) / rel_key).resolve().as_uri()


def label_studio_storages() -> list:
    """S3 storage config for Label Studio when input lives on S3; empty for local roots."""
    bucket = _input_s3_bucket()
    if bucket is None:
        return []
    from datapipe_label_studio.types import S3Bucket

    return [
        S3Bucket(
            bucket=bucket,
            key=AWS_KEY,
            secret=AWS_SECRET,
            region_name=AWS_REGION,
            endpoint_url=LABEL_STUDIO_S3_ENDPOINT_URL,
        )
    ]


def datapipe_tmp_folder() -> str:
    if _is_cloud_path(DATAPIPE_DIR):
        return str(Path(os.environ.get("DATAPIPE_E2E_TMP_DIR", "/tmp/datapipe-e2e")).resolve())
    return str(Path(DATAPIPE_DIR) / "tmp")


def _local_images_dir() -> Path:
    explicit = os.environ.get("LOCAL_IMAGES_DIR")
    if explicit:
        return Path(explicit).resolve()
    if _is_cloud_path(DATAPIPE_DIR):
        return Path("/tmp/datapipe-e2e/local_images").resolve()
    return (Path(DATAPIPE_DIR) / "local_images").resolve()


LOCAL_IMAGES_DIR = _local_images_dir()


DB_URL = os.environ.get("DB_URL")
if not DB_URL:
    raise RuntimeError(
        "DB_URL is required. Copy examples/e2e_template/.env.example to .env, "
        "start docker compose, and run: set -a && source .env && set +a"
    )

DB_SCHEMA = os.environ.get("DB_SCHEMA_CLASSIFICATION", "datapipe_e2e_classification")
DBCONN = DBConn(DB_URL, DB_SCHEMA)

CLICKHOUSE_RUN_LOGS_URL = os.environ.get("CLICKHOUSE_RUN_LOGS_URL")
if not CLICKHOUSE_RUN_LOGS_URL:
    raise RuntimeError(
        "CLICKHOUSE_RUN_LOGS_URL is required. Copy examples/e2e_template/.env.example to .env, "
        "start docker compose (clickhouse service), and run: set -a && source ../.env && set +a"
    )

from datapipe.executor import ExecutorConfig

_MAX_CPU = os.cpu_count() or 4


def _use_gpu() -> bool:
    explicit = os.environ.get("DATAPIPE_USE_GPU")
    if explicit is not None:
        return explicit.strip().lower() not in {"0", "false", "no", "off"}
    try:
        import tensorflow as tf

        return bool(tf.config.list_physical_devices("GPU"))
    except Exception:
        return False


def parallel_io_executor(*, parallelism_cap: int = 16, cpu_per_task: float = 0.2) -> ExecutorConfig:
    """Embarrassingly parallel I/O and image prep (download, upload)."""
    return ExecutorConfig(cpu=cpu_per_task, parallelism=min(_MAX_CPU, parallelism_cap))


def gpu_executor(*, parallelism: int | None = None) -> ExecutorConfig:
    """GPU when TensorFlow sees a CUDA device; CPU fallback otherwise."""
    if _use_gpu():
        return ExecutorConfig(gpu=1, cpu=1.0, parallelism=parallelism or 1)
    return ExecutorConfig(cpu=1.0, parallelism=min(_MAX_CPU, 4))


def metrics_executor(*, parallelism_cap: int = 8, cpu_per_task: float = 0.5) -> ExecutorConfig:
    """CPU-bound metrics aggregation over image batches."""
    return ExecutorConfig(cpu=cpu_per_task, parallelism=min(_MAX_CPU, parallelism_cap))


FIFTYONE_DATASET_NAME = "datapipe_classification_e2e"
