from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
from cv_pipeliner.core.data import BboxData, ImageData

from datapipe_ml.core.image_data import (
    convert_df_with_bbox_to_df_with_image_data,
    convert_df_with_image_data_to_df_with_bbox,
)
from datapipe_ml.frameworks.yolo.dataset import YOLOPoseDataConverter


def _image_data_with_bbox(**bbox_kwargs) -> ImageData:
    return ImageData(
        image=np.zeros((100, 100, 3), dtype=np.uint8),
        bboxes_data=[BboxData(**bbox_kwargs)],
    )


def test_bbox_data_has_keypoints_visibility_and_scores_fields():
    from cv_pipeliner.core.data import KeypointVisibility

    bbox = BboxData(
        xmin=0,
        ymin=0,
        xmax=10,
        ymax=10,
        keypoints=[[1, 2], [3, 4]],
        keypoints_visibility=[0, 2],
        keypoints_scores=[0.9, 0.8],
    )
    assert bbox.keypoints_visibility == [
        KeypointVisibility.NOT_LABELED,
        KeypointVisibility.LABELED_AND_VISIBLE,
    ]
    assert bbox.keypoints_scores == [0.9, 0.8]


def test_json_roundtrip_preserves_keypoints_visibility():
    df = pd.DataFrame(
        [
            {
                "image_id": "img1",
                "bboxes": [[0, 0, 100, 100]],
                "labels": ["person"],
                "keypoints": [[[10, 10], [90, 10], [90, 90], [10, 90]]],
                "keypoints_visibility": [[0, 1, 2, 0]],
            }
        ]
    )
    df_image = convert_df_with_bbox_to_df_with_image_data(df, primary_keys=["image_id"])
    bbox = df_image.iloc[0]["image_data"].bboxes_data[0]
    assert bbox.keypoints_visibility == [0, 1, 2, 0]

    df_back = convert_df_with_image_data_to_df_with_bbox(df_image, primary_keys=["image_id"])
    assert df_back.iloc[0]["keypoints_visibility"] == [[0, 1, 2, 0]]


def test_json_roundtrip_preserves_additional_info():
    df = pd.DataFrame(
        [
            {
                "image_id": "img1",
                "bboxes": [[0, 0, 100, 100], [10, 10, 50, 50]],
                "labels": ["person", "dog"],
                "bboxes_additional_infos": [{"keypoints_labels": ["nose", "ear"]}, {"source": "manual"}],
                "additional_info": {"subset_id": "train"},
            }
        ]
    )
    df_image = convert_df_with_bbox_to_df_with_image_data(df, primary_keys=["image_id"])
    image_data = df_image.iloc[0]["image_data"]
    assert image_data.additional_info == {"subset_id": "train"}
    assert image_data.bboxes_data[0].additional_info == {"keypoints_labels": ["nose", "ear"]}
    assert image_data.bboxes_data[1].additional_info == {"source": "manual"}

    df_back = convert_df_with_image_data_to_df_with_bbox(df_image, primary_keys=["image_id"])
    assert df_back.iloc[0]["bboxes_additional_infos"] == [{"keypoints_labels": ["nose", "ear"]}, {"source": "manual"}]
    assert df_back.iloc[0]["additional_info"] == {"subset_id": "train"}


def test_bbox_rows_roundtrip_preserves_additional_info():
    df = pd.DataFrame(
        [
            {
                "image_id": "img1",
                "bbox_id": "b1",
                "x_min": 0,
                "y_min": 0,
                "x_max": 10,
                "y_max": 10,
                "label": "person",
                "additional_info": {"track_id": 7},
            }
        ]
    )
    df_image = convert_df_with_bbox_to_df_with_image_data(df, primary_keys=["image_id"], bbox_id__name="bbox_id")
    image_data = df_image.iloc[0]["image_data"]
    assert image_data.additional_info == {}
    assert image_data.bboxes_data[0].additional_info == {"track_id": 7}

    df_back = convert_df_with_image_data_to_df_with_bbox(
        df_image, primary_keys=["image_id"], bbox_id__name="bbox_id"
    )
    assert df_back.iloc[0]["additional_info"] == {"track_id": 7}
    assert "image_additional_info" not in df_back.columns


def test_yolo_pose_annot_uses_visibility_field():
    converter = YOLOPoseDataConverter(class_names=["person"])
    image_data = _image_data_with_bbox(
        xmin=0,
        ymin=0,
        xmax=100,
        ymax=100,
        label="person",
        keypoints=[[10, 10], [90, 10], [90, 90]],
        keypoints_visibility=[0, 1, 2],
    )
    lines = converter.get_annot_from_image_data(image_data)
    parts = lines[0].split()
    assert [int(parts[i]) for i in (7, 10, 13)] == [0, 1, 2]


def test_yolo_pose_annot_raises_when_visibility_missing():
    converter = YOLOPoseDataConverter(class_names=["person"])
    image_data = _image_data_with_bbox(
        xmin=0,
        ymin=0,
        xmax=100,
        ymax=100,
        label="person",
        keypoints=[[10, 10], [90, 10]],
    )
    with pytest.raises(ValueError, match="keypoints_visibility is required"):
        converter.get_annot_from_image_data(image_data)


def test_df_to_yolo_preserves_mixed_visibility():
    df = pd.DataFrame(
        [
            {
                "image_id": "img1",
                "bboxes": [[0, 0, 100, 100]],
                "labels": ["person"],
                "keypoints": [[[10, 10], [90, 10], [50, 50]]],
                "keypoints_visibility": [[0, 1, 2]],
            }
        ]
    )
    df_image = convert_df_with_bbox_to_df_with_image_data(df, primary_keys=["image_id"])
    image_data = df_image.iloc[0]["image_data"]
    image_data.image = np.zeros((100, 100, 3), dtype=np.uint8)
    lines = YOLOPoseDataConverter(class_names=["person"]).get_annot_from_image_data(image_data)
    parts = lines[0].split()
    assert [int(parts[i]) for i in (7, 10, 13)] == [0, 1, 2]
