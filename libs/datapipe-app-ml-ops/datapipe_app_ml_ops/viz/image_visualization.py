from __future__ import annotations

import io
from collections.abc import Sequence
from typing import Any, Literal

import httpx
import numpy as np
from PIL import Image

OverlayMode = Literal["record", "gt", "prediction", "both", "plain"]


def _as_aligned_list(value: Any, *, length: int, default: Any) -> list[Any]:
    """Align a per-bbox field to ``length``, accepting scalar classification values."""
    if value is None:
        return [default] * length
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
        if length <= 0:
            return []
        return [value] + [default] * (length - 1)
    values = list(value)
    if len(values) != length:
        values = (values + [default] * length)[:length]
    return values


def iter_bbox_fields_from_record(
    record: dict[str, Any],
    *,
    scores_column: str | None = None,
) -> list[tuple[Any, Any | None, Any, Any, Any, Any]]:
    bboxes = record.get("bboxes") or []
    if not isinstance(bboxes, Sequence) or isinstance(bboxes, (str, bytes)):
        bboxes = []
    scores_key = scores_column or "prediction__detection_scores"
    labels = _as_aligned_list(record.get("labels"), length=len(bboxes), default=None)
    scores = _as_aligned_list(record.get(scores_key), length=len(bboxes), default=None)
    return [
        (label, score, xmin, ymin, xmax, ymax)
        for (xmin, ymin, xmax, ymax), label, score in zip(bboxes, labels, scores)
    ]


def _cv_pipeliner():
    from cv_pipeliner import (
        ImageData,
        ImageDataMatching,
        visualize_image_data,
        visualize_image_data_matching_side_by_side,
    )
    from cv_pipeliner.core.data import BboxData

    return ImageData, visualize_image_data, BboxData, ImageDataMatching, visualize_image_data_matching_side_by_side


def _aligned_per_bbox_field(record: dict[str, Any], field: str, *, length: int, default: Any) -> list[Any]:
    return _as_aligned_list(record.get(field), length=length, default=default)


def bboxes_data_from_record(record: dict[str, Any], *, scores_column: str | None = None) -> list[Any]:
    _, _, BboxData, _, _ = _cv_pipeliner()
    bboxes = record.get("bboxes") or []
    if not bboxes:
        return []

    labels = _aligned_per_bbox_field(record, "labels", length=len(bboxes), default=None)
    scores_key = scores_column or "prediction__detection_scores"
    scores = _aligned_per_bbox_field(record, scores_key, length=len(bboxes), default=None)
    keypoints = _aligned_per_bbox_field(record, "keypoints", length=len(bboxes), default=[])
    masks = _aligned_per_bbox_field(record, "masks", length=len(bboxes), default=[])
    keypoints_scores = _aligned_per_bbox_field(
        record,
        "prediction__keypoints_scores",
        length=len(bboxes),
        default=None,
    )
    keypoints_visibility = _aligned_per_bbox_field(
        record,
        "keypoints_visibility",
        length=len(bboxes),
        default=None,
    )

    result: list[Any] = []
    for (
        (xmin, ymin, xmax, ymax),
        label,
        score,
        bbox_keypoints,
        mask,
        bbox_keypoints_scores,
        bbox_keypoints_visibility,
    ) in zip(
        bboxes,
        labels,
        scores,
        keypoints,
        masks,
        keypoints_scores,
        keypoints_visibility,
    ):
        result.append(
            BboxData(
                xmin=xmin,
                ymin=ymin,
                xmax=xmax,
                ymax=ymax,
                label=label,
                keypoints=bbox_keypoints,
                keypoints_visibility=bbox_keypoints_visibility,
                keypoints_scores=bbox_keypoints_scores,
                mask=mask,
                detection_score=score,
            )
        )
    return result


def record_to_image_data(
    record: dict[str, Any] | None,
    *,
    image_url: str | None,
    scores_column: str | None = None,
) -> Any | None:
    ImageData, _, _, _, _ = _cv_pipeliner()
    if image_url is None:
        return None
    bboxes_data = bboxes_data_from_record(record or {}, scores_column=scores_column) if record else []
    return ImageData(image_path=image_url, bboxes_data=bboxes_data)


def load_image_bytes(image_url: str) -> bytes:
    if image_url.startswith(("http://", "https://")):
        response = httpx.get(image_url, timeout=30.0, follow_redirects=True)
        response.raise_for_status()
        return response.content
    with open(image_url, "rb") as handle:
        return handle.read()


def _resize_numpy_image(image: np.ndarray, max_side: int | None) -> np.ndarray:
    if max_side is None:
        return image
    height, width = image.shape[:2]
    longest = max(height, width)
    if longest <= max_side:
        return image
    scale = max_side / float(longest)
    new_size = (max(int(width * scale), 1), max(int(height * scale), 1))
    pil_image = Image.fromarray(image)
    return np.asarray(pil_image.resize(new_size, Image.Resampling.LANCZOS))


def _numpy_to_png_bytes(image: np.ndarray) -> tuple[bytes, str]:
    if image.dtype != np.uint8:
        image = image.astype(np.uint8)
    if image.ndim == 2:
        pil_image = Image.fromarray(image, mode="L")
    else:
        pil_image = Image.fromarray(image)
    buffer = io.BytesIO()
    pil_image.save(buffer, format="PNG")
    return buffer.getvalue(), "image/png"


def render_visualization_bytes(
    *,
    image_url: str,
    record: dict[str, Any] | None = None,
    gt_record: dict[str, Any] | None = None,
    prediction_record: dict[str, Any] | None = None,
    overlay: OverlayMode = "record",
    scores_column: str | None = None,
    annotations: bool = True,
    max_side: int | None = None,
) -> tuple[bytes, str]:
    if overlay == "plain" or not annotations:
        image = Image.open(io.BytesIO(load_image_bytes(image_url))).convert("RGB")
        if max_side is not None:
            image.thumbnail((max_side, max_side), Image.Resampling.LANCZOS)
        buffer = io.BytesIO()
        image.save(buffer, format="PNG")
        return buffer.getvalue(), "image/png"

    ImageData, visualize_image_data, _, ImageDataMatching, visualize_image_data_matching_side_by_side = _cv_pipeliner()

    if overlay == "both":
        gt_image_data = record_to_image_data(gt_record, image_url=image_url, scores_column=scores_column)
        pred_image_data = record_to_image_data(prediction_record, image_url=image_url, scores_column=scores_column)
        if gt_image_data is None or pred_image_data is None:
            raise ValueError("GT and prediction records are required for overlay=both")
        matching = ImageDataMatching(
            true_image_data=gt_image_data,
            pred_image_data=pred_image_data,
            minimum_iou=0.5,
        )
        rendered = visualize_image_data_matching_side_by_side(
            image_data_matching=matching,
            error_type="detection",
            include_additional_bboxes_data=True,
            true_include_labels=True,
            pred_include_labels=True,
            true_include_keypoints=True,
            pred_include_keypoints=True,
            true_include_keypoint_scores=False,
            pred_include_keypoint_scores=True,
            true_include_mask=True,
            pred_include_mask=True,
            mask_alpha=0.5,
            fontsize=24,
            thickness=2,
            keypoints_radius=2,
        )
        return _numpy_to_png_bytes(_resize_numpy_image(rendered, max_side))

    source_record = record
    if overlay == "gt":
        source_record = gt_record
    elif overlay == "prediction":
        source_record = prediction_record
    elif overlay == "record":
        source_record = record

    image_data = record_to_image_data(source_record, image_url=image_url, scores_column=scores_column)
    if image_data is None:
        raise ValueError("image_url is required for visualization")
    rendered = visualize_image_data(
        image_data,
        include_additional_bboxes_data=True,
        include_labels=True,
        include_keypoints=True,
        include_keypoint_scores=True,
        include_mask=True,
        keypoints_radius=2,
        mask_alpha=0.5,
        fontsize=24,
        thickness=2,
    )
    return _numpy_to_png_bytes(_resize_numpy_image(rendered, max_side))
