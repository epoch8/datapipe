from __future__ import annotations

import os
from pathlib import Path

from datapipe.store.database import DBConn

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

PROJECT_NAME = "Datapipe detection e2e"
LABEL_CONFIG = """
<View>
  <Image name="image" value="$image_url"/>
  <RectangleLabels name="label" toName="image">
    <Label value="Cat" background="#FF6B6B"/>
    <Label value="Dog" background="#4ECDC4"/>
  </RectangleLabels>
</View>
"""

COCO_CLASSES = [
    "person", "bicycle", "car", "motorcycle", "airplane", "bus", "train",
    "truck", "boat", "traffic light", "fire hydrant", "stop sign",
    "parking meter", "bench", "bird", "cat", "dog", "horse", "sheep",
    "cow", "elephant", "bear", "zebra", "giraffe", "backpack", "umbrella",
    "handbag", "tie", "suitcase", "frisbee", "skis", "snowboard",
    "sports ball", "kite", "baseball bat", "baseball glove", "skateboard",
    "surfboard", "tennis racket", "bottle", "wine glass", "cup", "fork",
    "knife", "spoon", "bowl", "banana", "apple", "sandwich", "orange",
    "broccoli", "carrot", "hot dog", "pizza", "donut", "cake", "chair",
    "couch", "potted plant", "bed", "dining table", "toilet", "tv",
    "laptop", "mouse", "remote", "keyboard", "cell phone", "microwave",
    "oven", "toaster", "sink", "refrigerator", "book", "clock", "vase",
    "scissors", "teddy bear", "hair drier", "toothbrush",
]
CLASSES_TO_KEEP = {"cat", "dog"}

DETECTION_MODEL_CONFIG = {
    "detection_model_id": "cat_dog_yolo_smoke",
    "detection_model__type": "yolov8",
    "detection_model__model_path": "yolo11n.pt",
    "detection_model__input_size": [16, 16],
    "detection_model__class_names": COCO_CLASSES,
    "detection_model__score_threshold": 0.01,
}

DATAPIPE_DIR = Path(os.environ.get("DATAPIPE_E2E_DIR", "datapipe")).resolve()
DATAPIPE_DIR.mkdir(parents=True, exist_ok=True)
LOCAL_IMAGES_DIR = Path(os.environ.get("LOCAL_IMAGES_DIR", DATAPIPE_DIR / "local_images")).resolve()


DB_URL = os.environ.get("DB_URL")
if not DB_URL:
    raise RuntimeError(
        "DB_URL is required. Copy examples/e2e_template/.env.example to .env, "
        "start docker compose, and run: set -a && source .env && set +a"
    )

DB_SCHEMA = os.environ.get("DB_SCHEMA_DETECTION", "datapipe_e2e_detection")
DBCONN = DBConn(DB_URL, DB_SCHEMA)

FIFTYONE_DATASET_NAME = "datapipe_detection_e2e"
