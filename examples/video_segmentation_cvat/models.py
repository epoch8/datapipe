from __future__ import annotations

import logging
from typing import List, Optional

import cv2
import numpy as np
import torch
from cv_pipeliner import BboxData
from PIL import Image

from config import DEVICE, HF_TOKEN, SAM_MAX_DETECTIONS, SAM_SCORE_THRESHOLD

logger = logging.getLogger(__name__)

_processor = None


def ensure_hf_login() -> None:
    from huggingface_hub import login

    if HF_TOKEN:
        login(token=HF_TOKEN)
        return
    login()


def get_processor():
    global _processor
    if _processor is None:
        ensure_hf_login()
        from sam3.model.sam3_image_processor import Sam3Processor
        from sam3.model_builder import build_sam3_image_model

        model = build_sam3_image_model()
        _processor = Sam3Processor(model)
        logger.info("Loaded SAM3 model on device %s", DEVICE)
    return _processor


def _mask_to_polygon(mask: np.ndarray) -> Optional[np.ndarray]:
    mask_uint8 = (mask.astype(np.uint8) * 255) if mask.max() <= 1 else mask.astype(np.uint8)
    contours, _ = cv2.findContours(mask_uint8, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    if not contours:
        return None

    contour = max(contours, key=cv2.contourArea)
    epsilon = 0.005 * cv2.arcLength(contour, True)
    approx = cv2.approxPolyDP(contour, epsilon, True)
    if approx.shape[0] < 3:
        return None
    return approx.reshape(-1, 2).astype(np.int32)


def _to_numpy(value) -> np.ndarray:
    return np.asarray(value.detach().cpu().float().numpy())


def _to_scalar(value) -> float:
    array = _to_numpy(value)
    return float(array.reshape(-1)[0])


def infer_image(image: Image.Image, text_prompt: str) -> List[BboxData]:
    processor = get_processor()
    device_type = "cuda" if DEVICE.startswith("cuda") else "cpu"

    if device_type == "cuda":
        with torch.autocast(device_type, dtype=torch.bfloat16):
            inference_state = processor.set_image(image)
            output = processor.set_text_prompt(state=inference_state, prompt=text_prompt)
    else:
        inference_state = processor.set_image(image)
        output = processor.set_text_prompt(state=inference_state, prompt=text_prompt)

    masks = output.get("masks", [])
    boxes = output.get("boxes", [])
    scores = output.get("scores", [])

    if masks is None or boxes is None or scores is None:
        return []

    num_detections = min(len(masks), len(boxes), len(scores))
    detections: List[BboxData] = []

    indexed_scores = [(idx, _to_scalar(scores[idx])) for idx in range(num_detections)]
    indexed_scores.sort(key=lambda item: item[1], reverse=True)

    kept = 0
    for rank, (idx, score) in enumerate(indexed_scores):
        if score < SAM_SCORE_THRESHOLD:
            continue
        if kept >= SAM_MAX_DETECTIONS:
            break

        box = _to_numpy(boxes[idx]).reshape(-1)
        if box.size < 4:
            continue

        x_min, y_min, x_max, y_max = [float(v) for v in box[:4]]
        mask = _to_numpy(masks[idx])
        if mask.ndim == 3:
            mask = mask[0]
        polygon = _mask_to_polygon(mask)
        mask_polygons = [polygon] if polygon is not None else []

        detections.append(
            BboxData(
                xmin=x_min,
                ymin=y_min,
                xmax=x_max,
                ymax=y_max,
                detection_score=score,
                mask=mask_polygons,
                additional_info={"detection_id": str(rank)},
            )
        )
        kept += 1

    return detections
