from __future__ import annotations

import os
from pathlib import Path

import fsspec
from datapipe.store.database import DBConn
from pathy import Pathy

AWS_KEY = os.environ.get("AWS_ACCESS_KEY_ID", "")
AWS_SECRET = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")

S3_BUCKET = os.environ.get("S3_BUCKET", "")
S3_PREFIX = os.environ.get("S3_PREFIX", "images")
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
    <Label value="person" background="#4ECDC4"/>
  </RectangleLabels>
  <KeyPointLabels name="kp" toName="image" smart="true" strokeWidth="2">
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
CLASSES_TO_KEEP = {"person"}

KEYPOINTS_MODEL_CONFIG = {
    "keypoints_model_id": "person_yolo_pose_smoke",
    "keypoints_model__type": "yolov8_pose",
    "keypoints_model__model_path": "yolo11n-pose.pt",
    "keypoints_model__input_size": [16, 16],
    "keypoints_model__class_names": ["person"],
    "keypoints_model__score_threshold": 0.01,
}


def _is_cloud_path(path: str) -> bool:
    protocol, _ = fsspec.core.split_protocol(path)
    return protocol not in (None, "file")


def datapipe_working_dir() -> str:
    explicit = os.environ.get("DATAPIPE_E2E_DIR")
    if explicit:
        if _is_cloud_path(explicit):
            return explicit.rstrip("/")
        local = Path(explicit).resolve()
        local.mkdir(parents=True, exist_ok=True)
        return str(local)
    if S3_BUCKET:
        prefix = Path(os.environ.get("S3_DATAPIPE_PREFIX", f"{S3_PREFIX}/datapipe"))
        return str(Pathy(f"s3://{S3_BUCKET}").joinpath(*prefix.parts))
    local = Path("datapipe").resolve()
    local.mkdir(parents=True, exist_ok=True)
    return str(local)


DATAPIPE_DIR = datapipe_working_dir()


def datapipe_filedir_fsspec_kwargs() -> dict | None:
    if not _is_cloud_path(DATAPIPE_DIR):
        return None
    from datapipe_ml.utils.fsspec_storage import s3_filedir_fsspec_kwargs

    return s3_filedir_fsspec_kwargs()


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

FIFTYONE_DATASET_NAME = "datapipe_keypoints_e2e"
