from __future__ import annotations

from pathlib import Path

from examples.e2e_template.common import ServiceSettings

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
    "person",
    "bicycle",
    "car",
    "motorcycle",
    "airplane",
    "bus",
    "train",
    "truck",
    "boat",
    "traffic light",
    "fire hydrant",
    "stop sign",
    "parking meter",
    "bench",
    "bird",
    "cat",
    "dog",
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


def local_images_dir(settings: ServiceSettings) -> Path:
    assert settings.local_images_dir is not None
    return settings.local_images_dir

