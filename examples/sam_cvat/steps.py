from __future__ import annotations

import shutil
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Iterator

import pandas as pd
from cv_pipeliner import BboxData
from PIL import Image
from tqdm import tqdm

from config import (
    CVAT_BOX_LABEL,
    CVAT_POLYGON_LABEL,
    HF_DATASET_CACHE_FOLDER,
    HF_DATASET_LABEL,
    HF_DATASET_NAME,
    HF_DATASET_SPLIT,
    IMAGE_SUFFIXES,
    INPUT_DIR,
    SAM_TEXT_PROMPT,
    TASK_QUEUE_ID,
)
from models import ensure_hf_login, infer_image


def _polygon_points_from_bbox(bbox: BboxData) -> list[list[float]] | None:
    if not isinstance(bbox.mask, list) or not bbox.mask:
        return None
    polygon = max(bbox.mask, key=lambda points: len(points)).reshape(-1, 2)
    if len(polygon) < 3:
        return None
    return [[float(x), float(y)] for x, y in polygon]


def _list_images_from_local_dir() -> list[dict[str, str]]:
    if INPUT_DIR is None or not INPUT_DIR.exists():
        return []

    records: list[dict[str, str]] = []
    for path in sorted(INPUT_DIR.iterdir()):
        if not path.is_file():
            continue
        if path.suffix.lower() not in IMAGE_SUFFIXES:
            continue
        records.append(
            {
                "image_id": path.stem,
                "image_path": str(path),
            }
        )
    return records


def _list_images_from_hf_dataset() -> list[dict[str, str]]:
    # HF datasets decode images into temporary files (item["image"].filename). Those paths
    # live under the system temp dir with random basenames that do not match what CVAT SDK
    # sends in upload_file_order, so CVAT upload fails with upload_file_order mismatch.
    # Materialize into HF_DATASET_CACHE_FOLDER with stable basenames before CVAT sees them.
    from datasets import load_dataset

    ds = load_dataset(HF_DATASET_NAME, split=HF_DATASET_SPLIT)
    ds = ds.filter(lambda x: x["label"] == HF_DATASET_LABEL and hasattr(x["image"], "filename"))

    HF_DATASET_CACHE_FOLDER.mkdir(parents=True, exist_ok=True)

    records: list[dict[str, str]] = []
    for idx, item in enumerate(ds):
        src_path = Path(item["image"].filename)
        suffix = src_path.suffix.lower() if src_path.suffix else ".jpg"
        if suffix not in IMAGE_SUFFIXES:
            continue

        image_id = f"hf_{idx:06d}"
        dest_path = HF_DATASET_CACHE_FOLDER / f"{image_id}{suffix}"
        if not dest_path.exists():
            if src_path.is_file():
                shutil.copy2(src_path, dest_path)
            else:
                item["image"].save(dest_path)

        records.append(
            {
                "image_id": image_id,
                "image_path": str(dest_path),
            }
        )
    return records


def list_local_images() -> Iterator[pd.DataFrame]:
    if INPUT_DIR is not None and INPUT_DIR.exists():
        records = _list_images_from_local_dir()
    else:
        records = _list_images_from_hf_dataset()

    if records:
        yield pd.DataFrame(records)
    else:
        yield pd.DataFrame(columns=["image_id", "image_path"])


def sam_inference(df_local_images: pd.DataFrame) -> pd.DataFrame:
    ensure_hf_login()
    if df_local_images.empty:
        return pd.DataFrame(
            columns=[
                "image_id",
                "detection_id",
                "score",
                "x_min",
                "y_min",
                "x_max",
                "y_max",
                "polygon_points",
            ]
        )

    records = []
    for _, row in df_local_images.iterrows():
        image = Image.open(row["image_path"]).convert("RGB")
        detections = infer_image(image, SAM_TEXT_PROMPT)
        for detection in detections:
            records.append(
                {
                    "image_id": row["image_id"],
                    "detection_id": detection.additional_info.get("detection_id", "0"),
                    "score": detection.detection_score,
                    "x_min": detection.xmin,
                    "y_min": detection.ymin,
                    "x_max": detection.xmax,
                    "y_max": detection.ymax,
                    "polygon_points": _polygon_points_from_bbox(detection),
                }
            )

    if not records:
        return pd.DataFrame(
            columns=[
                "image_id",
                "detection_id",
                "score",
                "x_min",
                "y_min",
                "x_max",
                "y_max",
                "polygon_points",
            ]
        )
    return pd.DataFrame(records)


def _polygon_points_to_cvat_attr(points: list[list[float]]) -> str:
    return ";".join(f"{x:.2f},{y:.2f}" for x, y in points)


def sam_to_cvat_xml(df_sam_predictions: pd.DataFrame) -> pd.DataFrame:
    if df_sam_predictions.empty:
        return pd.DataFrame(columns=["image_id", "annotations"])

    records = []
    for image_id, group in df_sam_predictions.groupby("image_id"):
        lines = ["<image>"]
        for _, row in group.iterrows():
            lines.append(
                f'        <box label="{CVAT_BOX_LABEL}" source="manual" occluded="0" '
                f'xtl="{row["x_min"]:.2f}" ytl="{row["y_min"]:.2f}" '
                f'xbr="{row["x_max"]:.2f}" ybr="{row["y_max"]:.2f}" z_order="0">'
            )
            lines.append("        </box>")

            polygon_points = row.get("polygon_points")
            if isinstance(polygon_points, list) and len(polygon_points) >= 3:
                points_attr = _polygon_points_to_cvat_attr(polygon_points)
                lines.append(
                    f'        <polygon label="{CVAT_POLYGON_LABEL}" source="manual" occluded="0" '
                    f'points="{points_attr}" z_order="0">'
                )
                lines.append("        </polygon>")
        lines.append("</image>")
        records.append({"image_id": image_id, "annotations": "\n".join(lines)})

    return pd.DataFrame(records, columns=["image_id", "annotations"])


def prepare_cvat_input(df_local_images: pd.DataFrame, df_sam_cvat_xml: pd.DataFrame) -> pd.DataFrame:
    if df_local_images.empty:
        return pd.DataFrame(columns=["image_id", "task_queue_id", "image_path", "annotations"])

    df = pd.merge(df_local_images, df_sam_cvat_xml, on="image_id", how="left")
    df["task_queue_id"] = TASK_QUEUE_ID
    df["image_path"] = df["image_path"].apply(lambda path: str(Path(path)))
    df["annotations"] = df["annotations"].fillna("<image></image>")
    return df[["image_id", "task_queue_id", "image_path", "annotations"]]


def _parse_points_attr(points_attr: str) -> list[list[float]]:
    points: list[list[float]] = []
    for pair in points_attr.split(";"):
        if not pair.strip():
            continue
        x_str, y_str = pair.split(",", 1)
        points.append([float(x_str), float(y_str)])
    return points


def parse_cvat_annotations(df_cvat_annotation: pd.DataFrame) -> pd.DataFrame:
    if df_cvat_annotation.empty:
        return pd.DataFrame(
            columns=[
                "image_id",
                "task_queue_id",
                "inner_task_id",
                "boxes",
                "polygons",
                "box_labels",
                "polygon_labels",
            ]
        )

    records = []
    for _, row in df_cvat_annotation.iterrows():
        annotation_xml = row.get("annotations") or ""
        if not annotation_xml.strip():
            continue

        try:
            image_element = ET.fromstring(annotation_xml)
        except ET.ParseError:
            continue

        boxes = []
        box_labels = []
        for box in image_element.findall("box"):
            boxes.append(
                {
                    "xtl": float(box.attrib.get("xtl", 0)),
                    "ytl": float(box.attrib.get("ytl", 0)),
                    "xbr": float(box.attrib.get("xbr", 0)),
                    "ybr": float(box.attrib.get("ybr", 0)),
                }
            )
            box_labels.append(box.attrib.get("label", ""))

        polygons = []
        polygon_labels = []
        for polygon in image_element.findall("polygon"):
            points_attr = polygon.attrib.get("points", "")
            if not points_attr:
                continue
            polygons.append(_parse_points_attr(points_attr))
            polygon_labels.append(polygon.attrib.get("label", ""))

        if not boxes and not polygons:
            continue

        records.append(
            {
                "image_id": row["image_id"],
                "task_queue_id": row["task_queue_id"],
                "inner_task_id": int(row["inner_task_id"]),
                "boxes": boxes,
                "polygons": polygons,
                "box_labels": box_labels,
                "polygon_labels": polygon_labels,
            }
        )

    if not records:
        return pd.DataFrame(
            columns=[
                "image_id",
                "task_queue_id",
                "inner_task_id",
                "boxes",
                "polygons",
                "box_labels",
                "polygon_labels",
            ]
        )
    return pd.DataFrame(records)
