from __future__ import annotations

from pathlib import Path

from examples.e2e_template.common import ServiceSettings

PROJECT_NAME = "Datapipe keypoints e2e"
LABEL_CONFIG = """
<View>
  <Image name="image" value="$image_url"/>
  <RectangleLabels name="bbox" toName="image">
    <Label value="person" background="#4ECDC4"/>
  </RectangleLabels>
  <KeyPointLabels name="kp" toName="image" smart="true">
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


def local_images_dir(settings: ServiceSettings) -> Path:
    assert settings.local_images_dir is not None
    return settings.local_images_dir

