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

PROJECT_NAME = "Datapipe keypoints e2e"
LABEL_CONFIG = """
<View>
  <Image name="image" value="$image_url"/>
  <RectangleLabels name="bbox" toName="image">
    <Label value="Person" background="#4ECDC4"/>
  </RectangleLabels>
  <KeyPointLabels name="kp" toName="image" smart="true" strokeWidth="0.5">
    <Label value="nose" background="#1f77b4"/>
    <Label value="left_eye" background="#ff7f0e"/>
    <Label value="right_eye" background="#2ca02c"/>
    <Label value="left_ear" background="#d62728"/>
    <Label value="right_ear" background="#9467bd"/>
    <Label value="left_shoulder" background="#8c564b"/>
    <Label value="right_shoulder" background="#e377c2"/>
    <Label value="left_elbow" background="#7f7f7f"/>
    <Label value="right_elbow" background="#bcbd22"/>
    <Label value="left_wrist" background="#17becf"/>
    <Label value="right_wrist" background="#3182bd"/>
    <Label value="left_hip" background="#9ecae1"/>
    <Label value="right_hip" background="#e6550d"/>
    <Label value="left_knee" background="#fd8d3c"/>
    <Label value="right_knee" background="#31a354"/>
    <Label value="left_ankle" background="#74c476"/>
    <Label value="right_ankle" background="#756bb1"/>
  </KeyPointLabels>
</View>
"""

KEYPOINTS_LABELS = [
    "nose",
    "left_eye",
    "right_eye",
    "left_ear",
    "right_ear",
    "left_shoulder",
    "right_shoulder",
    "left_elbow",
    "right_elbow",
    "left_wrist",
    "right_wrist",
    "left_hip",
    "right_hip",
    "left_knee",
    "right_knee",
    "left_ankle",
    "right_ankle",
]
COCO_PERSON_KEYPOINT_FLIP_IDX = [0, 2, 1, 4, 3, 6, 5, 8, 7, 10, 9, 12, 11, 14, 13, 16, 15]
KEYPOINTS_CLASSES = ["Person"]
CLASSES_TO_KEEP = set(KEYPOINTS_CLASSES)

E2E_TEMPLATE_DIR = Path(__file__).resolve().parents[1]
DEFAULT_KEYPOINTS_MODEL_PATH = E2E_TEMPLATE_DIR / "sample_data" / "models" / "yolo11n-pose.pt"

KEYPOINTS_MODEL_CONFIG = {
    "keypoints_model_id": "person_yolo_pose_smoke",
    "keypoints_model__type": "yolov8_pose",
    "keypoints_model__model_path": str(DEFAULT_KEYPOINTS_MODEL_PATH),
    "keypoints_model__input_size": [16, 16],
    "keypoints_model__class_names": ["Person"],
    "keypoints_model__score_threshold": 0.01,
}


def _is_cloud_path(path: str) -> bool:
    protocol, _ = fsspec.core.split_protocol(path)
    return protocol not in (None, "file")


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

DB_SCHEMA = os.environ.get("DB_SCHEMA_KEYPOINTS", "datapipe_e2e_keypoints")
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
        import torch

        return torch.cuda.is_available()
    except Exception:
        return False


def parallel_io_executor(*, parallelism_cap: int = 16, cpu_per_task: float = 0.2) -> ExecutorConfig:
    """Embarrassingly parallel I/O and image prep (resize, download, upload)."""
    return ExecutorConfig(cpu=cpu_per_task, parallelism=min(_MAX_CPU, parallelism_cap))


def gpu_executor(*, parallelism: int | None = None) -> ExecutorConfig:
    """GPU when CUDA is available; CPU fallback otherwise."""
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

FIFTYONE_DATASET_NAME = "datapipe_keypoints_e2e"
