from dataclasses import dataclass
from typing import Any, List, Optional, Tuple

import cv2
import numpy as np
from cv_pipeliner import ImageData, PipelineInferencer
from cv_pipeliner.core.data import BboxData


@dataclass
class CropBlock:
    rect: Tuple[int, int, int, int]
    accept_rect: Tuple[int, int, int, int]


def build_crop_blocks(
    width: int,
    height: int,
    h_crossing: int,
    v_crossing: int,
    threshold_space: int,
    block_width: int,
    block_height: int,
) -> List[CropBlock]:
    if block_width > width - 1:
        raise ValueError(f"{block_width=} is greater than {width=} of image")
    if block_height > height - 1:
        raise ValueError(f"{block_height=} is greater than {height=} of image")

    remaining_width = 0
    remaining_height = height
    block_top = 0
    row_remove_top = 0
    row_remove_bottom = 0
    blocks = []

    while remaining_width > 0 or remaining_height > 0:
        if remaining_width == 0 and remaining_height > 0:
            remaining_width = width
            if remaining_height == height:
                block_top = 0
                remaining_height -= block_height - v_crossing
                row_remove_top = 0
                row_remove_bottom = v_crossing - threshold_space
            elif remaining_height >= block_height:
                block_top = height - remaining_height
                remaining_height -= block_height - v_crossing
                row_remove_top = threshold_space
                row_remove_bottom = v_crossing - threshold_space
            else:
                block_top = height - block_height
                row_remove_top = threshold_space + (block_height - remaining_height)
                row_remove_bottom = 0
                remaining_height = 0

        block_left = 0
        remove_left = 0
        remove_right = 0
        if remaining_width == width:
            block_left = 0
            remaining_width -= block_width - h_crossing
            remove_left = 0
            remove_right = h_crossing - threshold_space
        elif remaining_width >= block_width:
            block_left = width - remaining_width
            remaining_width -= block_width - h_crossing
            remove_left = threshold_space
            remove_right = h_crossing - threshold_space
        elif remaining_width > 0:
            block_left = width - block_width
            remove_left = threshold_space + (block_width - remaining_width)
            remove_right = 0
            remaining_width = 0
        else:
            raise RuntimeError("Unexpected crop-block state")

        blocks.append(
            CropBlock(
                rect=(block_left, block_top, block_width, block_height),
                accept_rect=(remove_left, row_remove_top, block_width - remove_right, block_height - row_remove_bottom),
            )
        )
    return blocks


def predict_bbox_like_by_crops(
    image_data: ImageData,
    inferencer: PipelineInferencer,
    detection_score_threshold: float,
    h_crossing: int,
    v_crossing: int,
    threshold_space: int,
    block_width: int,
    block_height: int,
    model_input_size: Tuple[int, int],
    include_masks: bool = False,
    include_keypoints: bool = False,
) -> ImageData:
    width, height = image_data.get_image_size()
    blocks = build_crop_blocks(width, height, h_crossing, v_crossing, threshold_space, block_width, block_height)
    image = image_data.open_image()
    crops = []
    for block in blocks:
        x1, y1, crop_width, crop_height = block.rect
        image_crop = image[y1 : y1 + crop_height, x1 : x1 + crop_width]
        image_crop = cv2.resize(image_crop, model_input_size, interpolation=cv2.INTER_AREA)
        crops.append(ImageData(image=image_crop, bboxes_data=[]))

    predicted_crops: List[ImageData] = inferencer.predict(
        crops, detection_score_threshold=detection_score_threshold, disable_tqdm=True
    )

    bboxes_data = []
    scale_x = block_width / model_input_size[0]
    scale_y = block_height / model_input_size[1]
    for pred_image_data, block in zip(predicted_crops, blocks):
        block_left, block_top, _, _ = block.rect
        remove_left, row_remove_top, accept_width, accept_height = block.accept_rect
        block_x1 = block_left + remove_left
        block_y1 = block_top + row_remove_top
        block_x2 = block_left + accept_width
        block_y2 = block_top + accept_height

        for bbox_data in pred_image_data.bboxes_data:
            pred_xmin = bbox_data.xmin * scale_x + block_left
            pred_ymin = bbox_data.ymin * scale_y + block_top
            pred_xmax = bbox_data.xmax * scale_x + block_left
            pred_ymax = bbox_data.ymax * scale_y + block_top
            if not (block_x1 <= (pred_xmin + pred_xmax) / 2 <= block_x2):
                continue
            if not (block_y1 <= (pred_ymin + pred_ymax) / 2 <= block_y2):
                continue

            kwargs: dict[str, Any] = {}
            if include_masks:
                kwargs["mask"] = [
                    [[x * scale_x + block_left, y * scale_y + block_top] for (x, y) in polygon]
                    for polygon in bbox_data.mask
                ]
            if include_keypoints and bbox_data.keypoints is not None:
                kwargs["keypoints"] = np.array(
                    [[x * scale_x + block_left, y * scale_y + block_top] for (x, y) in bbox_data.keypoints]
                ).reshape(-1, 2)
                if bbox_data.keypoints_scores is not None:
                    keypoint_scores = bbox_data.keypoints_scores
                    if isinstance(keypoint_scores, np.ndarray):
                        keypoint_scores = keypoint_scores.tolist()
                    kwargs["keypoints_scores"] = keypoint_scores
                if bbox_data.keypoints_visibility is not None:
                    visibility = bbox_data.keypoints_visibility
                    if isinstance(visibility, np.ndarray):
                        visibility = visibility.tolist()
                    kwargs["keypoints_visibility"] = visibility
            bboxes_data.append(
                BboxData(
                    image_path=bbox_data.image_path,
                    meta_height=height,
                    meta_width=width,
                    xmin=int(pred_xmin),
                    ymin=int(pred_ymin),
                    xmax=int(pred_xmax),
                    ymax=int(pred_ymax),
                    label=bbox_data.label,
                    labels_top_n=bbox_data.labels_top_n,
                    classification_scores_top_n=bbox_data.classification_scores_top_n,
                    detection_score=bbox_data.detection_score,
                    **kwargs,
                )
            )

    return ImageData(image_path=image_data.image_path, bboxes_data=bboxes_data)


def predict_by_crops(
    image_data: ImageData,
    inferencer: PipelineInferencer,
    detection_score_threshold: float,
    hCrossing: int,
    vCrossing: int,
    thresholdSpace: int,
    blockWidth: int,
    blockHeight: int,
    model_input_size: Tuple[int, int] = (640, 640),
    *,
    threseholdSpace: Optional[int] = None,
    include_masks: bool = False,
    include_keypoints: bool = False,
) -> ImageData:
    from datapipe_ml.inference.common import resolve_threshold_space

    resolved_threshold_space = resolve_threshold_space(
        threshold_space=thresholdSpace,
        thresehold_space=threseholdSpace,
    )
    assert resolved_threshold_space is not None
    return predict_bbox_like_by_crops(
        image_data=image_data,
        inferencer=inferencer,
        detection_score_threshold=detection_score_threshold,
        h_crossing=hCrossing,
        v_crossing=vCrossing,
        threshold_space=resolved_threshold_space,
        block_width=blockWidth,
        block_height=blockHeight,
        model_input_size=model_input_size,
        include_masks=include_masks,
        include_keypoints=include_keypoints,
    )
