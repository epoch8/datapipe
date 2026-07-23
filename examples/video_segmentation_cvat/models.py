from __future__ import annotations

import logging
import os
from typing import List, Optional

import cv2
import numpy as np
import torch
from cv_pipeliner import BboxData
from PIL import Image

from config import (
    DEVICE,
    HF_TOKEN,
    SAM_MAX_DETECTIONS,
    SAM_MAX_INFER_SIDE,
    SAM_SCORE_THRESHOLD,
)

logger = logging.getLogger(__name__)

_processor = None


def ensure_hf_login() -> None:
    # huggingface_hub reads HF_TOKEN from the environment for gated-model downloads, so when a token
    # is configured we just make sure it is exported and skip login() -- login() persists the token to
    # ~/.cache/huggingface, which fails on hosts with a read-only home dir. Fall back to interactive
    # login only when no token is set.
    if HF_TOKEN:
        os.environ["HF_TOKEN"] = HF_TOKEN
        return
    from huggingface_hub import login

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


def _is_oom(exc: BaseException) -> bool:
    return isinstance(exc, torch.cuda.OutOfMemoryError) or (
        isinstance(exc, RuntimeError) and "out of memory" in str(exc).lower()
    )


def _run_sam(image: Image.Image, text_prompt: str, device_type: str) -> dict:
    processor = get_processor()
    try:
        if device_type == "cuda":
            with torch.autocast(device_type, dtype=torch.bfloat16):
                inference_state = processor.set_image(image)
                return processor.set_text_prompt(state=inference_state, prompt=text_prompt)
        inference_state = processor.set_image(image)
        return processor.set_text_prompt(state=inference_state, prompt=text_prompt)
    finally:
        # Release cached blocks after every attempt so fragmentation doesn't accumulate over a long
        # run and a failed attempt frees its memory before the next (smaller) retry.
        if device_type == "cuda":
            torch.cuda.empty_cache()


def infer_image(image: Image.Image, text_prompt: str) -> List[BboxData]:
    device_type = "cuda" if DEVICE.startswith("cuda") else "cpu"

    # SAM3 emits masks at the input resolution, so a full 720p frame needs far more VRAM than a small
    # GPU has. Downscale before inference (detections are mapped back to the original frame's
    # coordinates, since the full-res frame is what goes to CVAT). A crowded frame can still spike
    # past a tight card, so on OOM we retry at progressively smaller sizes rather than fail the run.
    orig_w, orig_h = image.size
    longest = max(orig_w, orig_h)
    target = SAM_MAX_INFER_SIDE if SAM_MAX_INFER_SIDE else longest
    caps: List[int] = []
    for cap in (target, 512, 384):
        cap = min(cap, longest)
        if cap not in caps:
            caps.append(cap)

    output = None
    inv_scale = 1.0
    for cap in caps:
        scale = cap / longest
        if scale < 1.0:
            sized = image.resize(
                (max(1, round(orig_w * scale)), max(1, round(orig_h * scale))),
                Image.BILINEAR,
            )
        else:
            sized = image
        try:
            output = _run_sam(sized, text_prompt, device_type)
            inv_scale = 1.0 / scale
            break
        except Exception as exc:  # noqa: BLE001 - only OOM is retried, everything else re-raises
            if not _is_oom(exc):
                raise
            logger.warning(
                "SAM OOM on a %dx%d frame at longest-side<=%d; retrying smaller", orig_w, orig_h, cap
            )

    if output is None:
        logger.warning("SAM OOM on a %dx%d frame at every size; skipping frame", orig_w, orig_h)
        return []

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

        x_min, y_min, x_max, y_max = [float(v) * inv_scale for v in box[:4]]
        mask = _to_numpy(masks[idx])
        if mask.ndim == 3:
            mask = mask[0]
        polygon = _mask_to_polygon(mask)
        if polygon is not None and scale != 1.0:
            polygon = (polygon.astype(np.float32) * inv_scale).astype(np.int32)
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
