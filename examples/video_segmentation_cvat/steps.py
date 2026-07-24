from __future__ import annotations

import subprocess
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Iterator

import imagehash
import pandas as pd
from cv_pipeliner import BboxData
from PIL import Image

from config import (
    CVAT_BOX_LABEL,
    CVAT_POLYGON_LABEL,
    FFMPEG_BIN,
    FRAMES_DIR,
    IMAGES_DIR,
    INPUT_VIDEO_DIR,
    PHASH_MAX_DISTANCE,
    PHASH_SIZE,
    SAM_MAX_INFER_SIDE,
    SAM_TEXT_PROMPT,
    SAMPLE_FPS,
    TASK_QUEUE_ID,
    VIDEO_SUFFIXES,
)
from models import infer_image

SAM_CONFIG_ID = "default"

_LOCAL_IMAGES_COLUMNS = ["video_id", "image_id", "image_path"]
_DEDUPED_FRAMES_COLUMNS = ["video_id", "image_id", "frame_path"]
_FRAMES_COLUMNS = ["video_id", "frame_id", "ts_sec", "frame_path"]
_SAM_PRED_COLUMNS = [
    "image_id",
    "detection_id",
    "score",
    "x_min",
    "y_min",
    "x_max",
    "y_max",
    "polygon_points",
]


# --- video -> frames -----------------------------------------------------------------------------


def list_videos() -> Iterator[pd.DataFrame]:
    records: list[dict[str, str]] = []
    if INPUT_VIDEO_DIR is not None and INPUT_VIDEO_DIR.exists():
        for path in sorted(INPUT_VIDEO_DIR.iterdir()):
            if path.is_file() and path.suffix.lower() in VIDEO_SUFFIXES:
                records.append({"video_id": path.stem, "video_path": str(path)})

    if records:
        yield pd.DataFrame(records)
    else:
        yield pd.DataFrame(columns=["video_id", "video_path"])


def _extract_one_video(video_id: str, video_path: str) -> list[dict]:
    out_dir = FRAMES_DIR / video_id
    out_dir.mkdir(parents=True, exist_ok=True)

    existing = sorted(out_dir.glob("f_*.jpg"))
    if not existing:
        # -vf fps=N samples N frames per second; VFR keeps timestamps honest.
        cmd = [
            FFMPEG_BIN,
            "-hide_banner",
            "-loglevel",
            "error",
            "-i",
            video_path,
            "-vf",
            f"fps={SAMPLE_FPS}",
            "-q:v",
            "2",
            str(out_dir / "f_%06d.jpg"),
        ]
        subprocess.run(cmd, check=True)
        existing = sorted(out_dir.glob("f_*.jpg"))

    records = []
    for path in existing:
        n = int(path.stem.split("_")[-1])  # f_000123 -> 123 (1-based)
        records.append(
            {
                "frame_id": f"{video_id}_{n:06d}",
                "video_id": video_id,
                "ts_sec": (n - 1) / SAMPLE_FPS,
                "frame_path": str(path),
            }
        )
    return records


def extract_frames(df_video: pd.DataFrame) -> pd.DataFrame:
    if df_video.empty:
        return pd.DataFrame(columns=_FRAMES_COLUMNS)

    records: list[dict] = []
    for _, row in df_video.iterrows():
        records.extend(_extract_one_video(str(row["video_id"]), str(row["video_path"])))

    if not records:
        return pd.DataFrame(columns=_FRAMES_COLUMNS)
    return pd.DataFrame(records, columns=_FRAMES_COLUMNS)


def dedup_frames(df_frames: pd.DataFrame) -> pd.DataFrame:
    """Perceptual-hash dedup per video: keep a frame only if it differs from the last kept frame by
    more than PHASH_MAX_DISTANCE (Hamming). Walking POV frames are highly redundant, so a sequential
    compare-to-last-kept pass removes near-duplicates cheaply while preserving genuine scene change."""
    if df_frames.empty:
        return pd.DataFrame(columns=_DEDUPED_FRAMES_COLUMNS)

    records: list[dict] = []
    for video_id, group in df_frames.groupby("video_id"):
        ordered = group.sort_values(["ts_sec", "frame_id"])
        last_hash = None
        for _, row in ordered.iterrows():
            try:
                with Image.open(row["frame_path"]) as img:
                    current = imagehash.phash(img.convert("RGB"), hash_size=PHASH_SIZE)
            except (OSError, ValueError):
                continue
            if last_hash is not None and (current - last_hash) <= PHASH_MAX_DISTANCE:
                continue
            last_hash = current
            records.append(
                {
                    "video_id": str(video_id),
                    "image_id": row["frame_id"],
                    "frame_path": row["frame_path"],
                }
            )

    if not records:
        return pd.DataFrame(columns=_DEDUPED_FRAMES_COLUMNS)
    return pd.DataFrame(records, columns=_DEDUPED_FRAMES_COLUMNS)


def downscale_frames(df_deduped: pd.DataFrame) -> pd.DataFrame:
    """Resize each deduped frame so its longest side is at most SAM_MAX_INFER_SIDE, writing the result
    under IMAGES_DIR. SAM3 emits masks at the input resolution, so full 720p+ frames blow past a small
    GPU; the resized frame is what both SAM and CVAT use, so detection coordinates line up. With
    SAM_MAX_INFER_SIDE=0 the original frame is passed through unchanged (use only on a roomy GPU)."""
    if df_deduped.empty:
        return pd.DataFrame(columns=_LOCAL_IMAGES_COLUMNS)

    records: list[dict] = []
    for _, row in df_deduped.iterrows():
        frame_path = str(row["frame_path"])
        if not SAM_MAX_INFER_SIDE:
            image_path = frame_path
        else:
            out_dir = IMAGES_DIR / str(row["video_id"])
            out_dir.mkdir(parents=True, exist_ok=True)
            out_path = out_dir / f"{row['image_id']}.jpg"
            if not out_path.exists():
                with Image.open(frame_path) as img:
                    img = img.convert("RGB")
                    longest = max(img.size)
                    if longest > SAM_MAX_INFER_SIDE:
                        scale = SAM_MAX_INFER_SIDE / longest
                        img = img.resize(
                            (max(1, round(img.width * scale)), max(1, round(img.height * scale))),
                            Image.BILINEAR,
                        )
                    img.save(out_path, quality=95)
            image_path = str(out_path)
        records.append(
            {"video_id": str(row["video_id"]), "image_id": row["image_id"], "image_path": image_path}
        )

    return pd.DataFrame(records, columns=_LOCAL_IMAGES_COLUMNS)


# --- SAM config + inference (reused from sam_cvat) -----------------------------------------------


def list_sam_config() -> Iterator[pd.DataFrame]:
    yield pd.DataFrame([{"config_id": SAM_CONFIG_ID, "text_prompt": SAM_TEXT_PROMPT}])


def _polygon_points_from_bbox(bbox: BboxData) -> list[list[float]] | None:
    if not isinstance(bbox.mask, list) or not bbox.mask:
        return None
    polygon = max(bbox.mask, key=lambda points: len(points)).reshape(-1, 2)
    if len(polygon) < 3:
        return None
    return [[float(x), float(y)] for x, y in polygon]


def sam_inference(df_local_images: pd.DataFrame, df_sam_config: pd.DataFrame) -> pd.DataFrame:
    if df_local_images.empty:
        return pd.DataFrame(columns=_SAM_PRED_COLUMNS)

    if df_sam_config.empty:
        text_prompt = SAM_TEXT_PROMPT
    else:
        text_prompt = str(df_sam_config.iloc[0]["text_prompt"])

    records = []
    for _, row in df_local_images.iterrows():
        image = Image.open(row["image_path"]).convert("RGB")
        detections = infer_image(image, text_prompt)
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
        return pd.DataFrame(columns=_SAM_PRED_COLUMNS)
    return pd.DataFrame(records)


# --- SAM predictions -> CVAT XML -> CVAT input (reused from sam_cvat) -----------------------------


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
    # One CVAT task per video: scope the CVAT batch by video_id (falls back to a constant if absent).
    df["task_queue_id"] = df["video_id"] if "video_id" in df.columns else TASK_QUEUE_ID
    df["image_path"] = df["image_path"].apply(lambda path: str(Path(path)))
    df["annotations"] = df["annotations"].fillna("<image></image>")
    return df[["image_id", "task_queue_id", "image_path", "annotations"]]


# --- CVAT annotations -> datapipe (reused from sam_cvat) ------------------------------------------


def _parse_points_attr(points_attr: str) -> list[list[float]]:
    points: list[list[float]] = []
    for pair in points_attr.split(";"):
        if not pair.strip():
            continue
        x_str, y_str = pair.split(",", 1)
        points.append([float(x_str), float(y_str)])
    return points


def parse_cvat_annotations(df_cvat_annotation: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "image_id",
        "task_queue_id",
        "inner_task_id",
        "boxes",
        "polygons",
        "box_labels",
        "polygon_labels",
    ]
    if df_cvat_annotation.empty:
        return pd.DataFrame(columns=columns)

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
        return pd.DataFrame(columns=columns)
    return pd.DataFrame(records)
