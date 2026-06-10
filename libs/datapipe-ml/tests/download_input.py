#!/usr/bin/env python3
import json
import os
import random
import sys
import time
from pathlib import Path
from zipfile import ZipFile

import requests
from tqdm import tqdm

for parent in Path(__file__).resolve().parents:
    if (parent / "tools" / "sample_data" / "coco_cache.py").exists():
        sys.path.insert(0, str(parent))
        break
else:
    raise RuntimeError("Cannot find datapipe workspace root (expected tools/sample_data/coco_cache.py)")

from tools.sample_data.coco_cache import (
    COCO_ANN_ZIP_URL,
    INSTANCES_JSON,
    KEYPOINTS_JSON,
    coco_annotations_zip_path,
    ensure_coco_annotations_zip,
)

# ---------- settings ----------
OUT_DIR = Path("input")
IMAGES_DIR = OUT_DIR / "images"
LABELS_DIR = OUT_DIR / "labels"  # per-image JSON goes here
KPS_OUT_DIR = Path("input_kps")
KPS_IMAGES_DIR = KPS_OUT_DIR / "images"
KPS_LABELS_DIR = KPS_OUT_DIR / "labels"
NUM_IMAGES = int(os.environ.get("DATAPIPE_ML_DOWNLOAD_IMAGES", "100"))
NUM_KEYPOINT_IMAGES = int(os.environ.get("DATAPIPE_ML_DOWNLOAD_KEYPOINT_IMAGES", str(NUM_IMAGES)))
RNG_SEED = 1234

COCO_IMG_BASE = "http://images.cocodataset.org/train2017/"
ANN_JSON_PATH_IN_ZIP = INSTANCES_JSON
KEYPOINTS_ANN_JSON_PATH_IN_ZIP = KEYPOINTS_JSON
COCO_PERSON_KEYPOINT_FLIP_IDX = [0, 2, 1, 4, 3, 6, 5, 8, 7, 10, 9, 12, 11, 14, 13, 16, 15]

random.seed(RNG_SEED)
IMAGES_DIR.mkdir(parents=True, exist_ok=True)
LABELS_DIR.mkdir(parents=True, exist_ok=True)
KPS_IMAGES_DIR.mkdir(parents=True, exist_ok=True)
KPS_LABELS_DIR.mkdir(parents=True, exist_ok=True)


def get_with_retries(url: str, *, attempts: int = 5) -> requests.Response:
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            response = requests.get(url, timeout=60, stream=True)
            response.raise_for_status()
            return response
        except requests.RequestException as exc:
            last_error = exc
            if attempt == attempts:
                break
            time.sleep(min(2**attempt, 30))
    assert last_error is not None
    raise last_error


def download_with_tqdm(url: str, dst: Path, desc: str):
    tmp_dst = dst.with_suffix(f"{dst.suffix}.tmp")
    tmp_dst.unlink(missing_ok=True)
    with get_with_retries(url) as response:
        total = int(response.headers.get("content-length", 0))
        with open(tmp_dst, "wb") as f, tqdm(total=total, unit="B", unit_scale=True, desc=desc) as bar:
            for chunk in response.iter_content(8192):
                if chunk:
                    f.write(chunk)
                    bar.update(len(chunk))
    tmp_dst.replace(dst)


def is_polygon_segmentation(seg):
    return isinstance(seg, list) and all(isinstance(p, list) for p in seg)


def coco_bbox_xywh_to_xyxy(bb):
    # COCO [x,y,w,h] -> [xmin,ymin,xmax,ymax]
    x, y, w, h = bb
    return [int(x), int(y), int(x + w), int(y + h)]


def flatten_to_xy_pairs(flat):
    it = iter(flat)
    return [[int(x), int(y)] for x, y in zip(it, it)]


def clamp_bbox_xyxy(xyxy, width, height):
    xyxy[0] = max(0, min(xyxy[0], width))
    xyxy[1] = max(0, min(xyxy[1], height))
    xyxy[2] = max(0, min(xyxy[2], width))
    xyxy[3] = max(0, min(xyxy[3], height))
    return xyxy


def download_images(sample_ids, id_to_img, images_dir, desc):
    print(f"-> Downloading {len(sample_ids)} {desc} images...")
    for iid in tqdm(sample_ids, desc=desc):
        meta = id_to_img[iid]
        fname = meta["file_name"]
        url = COCO_IMG_BASE + fname
        outp = images_dir / fname
        if outp.exists():
            continue
        tmp_outp = outp.with_suffix(f"{outp.suffix}.tmp")
        tmp_outp.unlink(missing_ok=True)
        with get_with_retries(url) as r:
            total = int(r.headers.get("content-length", 0))
            with open(tmp_outp, "wb") as f, tqdm(total=total, unit="B", unit_scale=True, desc=fname, leave=False) as bar:
                for chunk in r.iter_content(8192):
                    if chunk:
                        f.write(chunk)
                        bar.update(len(chunk))
        tmp_outp.replace(outp)


def split_coco_keypoints(flat_keypoints):
    keypoints = []
    visibility = []
    for idx in range(0, len(flat_keypoints), 3):
        keypoints.append([int(flat_keypoints[idx]), int(flat_keypoints[idx + 1])])
        visibility.append(int(flat_keypoints[idx + 2]))
    return keypoints, visibility


def write_detection_segmentation_dataset(data):
    cat_id_to_name = {c["id"]: c["name"] for c in data["categories"]}
    id_to_img = {img["id"]: img for img in data["images"]}

    anns_by_image = {}
    for ann in data["annotations"]:
        bb = ann.get("bbox")
        seg = ann.get("segmentation")
        if not bb or not seg:
            continue
        if ann.get("iscrowd", 0) == 1:
            continue
        if not is_polygon_segmentation(seg):
            continue
        anns_by_image.setdefault(ann["image_id"], []).append(ann)

    valid_image_ids = [iid for iid, anns in anns_by_image.items() if len(anns) > 0]
    if len(valid_image_ids) < NUM_IMAGES:
        raise RuntimeError(f"Found fewer than {NUM_IMAGES} images with polygon masks + bboxes.")

    sample_ids = random.sample(valid_image_ids, NUM_IMAGES)
    download_images(sample_ids, id_to_img, IMAGES_DIR, "detection/segmentation")

    for iid in sample_ids:
        meta = id_to_img[iid]
        width, height = meta["width"], meta["height"]
        stem = Path(meta["file_name"]).stem

        bboxes, labels, masks = [], [], []
        for ann in anns_by_image[iid]:
            xyxy = clamp_bbox_xyxy(coco_bbox_xywh_to_xyxy(ann["bbox"]), width, height)
            bboxes.append(xyxy)

            labels.append(cat_id_to_name.get(ann["category_id"], str(ann["category_id"])))

            obj_polys = []
            for poly_flat in ann["segmentation"]:
                if len(poly_flat) >= 6:
                    obj_polys.append(flatten_to_xy_pairs(poly_flat))
            masks.append(obj_polys)

        per_image = {
            "bboxes": bboxes,
            "labels": labels,
            "masks": masks,
        }
        (LABELS_DIR / f"{stem}.json").write_text(json.dumps(per_image), encoding="utf-8")


def write_keypoints_dataset(data):
    id_to_img = {img["id"]: img for img in data["images"]}

    anns_by_image = {}
    for ann in data["annotations"]:
        bb = ann.get("bbox")
        keypoints = ann.get("keypoints")
        if not bb or not keypoints:
            continue
        if ann.get("iscrowd", 0) == 1:
            continue
        if ann.get("num_keypoints", 0) <= 0:
            continue
        anns_by_image.setdefault(ann["image_id"], []).append(ann)

    valid_image_ids = [iid for iid, anns in anns_by_image.items() if len(anns) > 0]
    if len(valid_image_ids) < NUM_KEYPOINT_IMAGES:
        raise RuntimeError(f"Found fewer than {NUM_KEYPOINT_IMAGES} images with person keypoints.")

    sample_ids = random.sample(valid_image_ids, NUM_KEYPOINT_IMAGES)
    download_images(sample_ids, id_to_img, KPS_IMAGES_DIR, "keypoints")

    for iid in sample_ids:
        meta = id_to_img[iid]
        width, height = meta["width"], meta["height"]
        stem = Path(meta["file_name"]).stem

        bboxes, labels, keypoints, keypoints_visibility = [], [], [], []
        for ann in anns_by_image[iid]:
            xyxy = clamp_bbox_xyxy(coco_bbox_xywh_to_xyxy(ann["bbox"]), width, height)
            kps, visibility = split_coco_keypoints(ann["keypoints"])
            bboxes.append(xyxy)
            labels.append("person")
            keypoints.append(kps)
            keypoints_visibility.append(visibility)

        per_image = {
            "bboxes": bboxes,
            "labels": labels,
            "keypoints": keypoints,
            "keypoints_visibility": keypoints_visibility,
            "flip_idx": COCO_PERSON_KEYPOINT_FLIP_IDX,
        }
        (KPS_LABELS_DIR / f"{stem}.json").write_text(json.dumps(per_image), encoding="utf-8")


def main():
    ann_zip = ensure_coco_annotations_zip(
        download=lambda dst: download_with_tqdm(COCO_ANN_ZIP_URL, dst, desc="Annotations")
    )

    # read COCO annotations directly from ZIP (no extraction to disk)
    print("-> Reading annotations from ZIP...")
    with ZipFile(ann_zip, "r") as zf:
        with zf.open(ANN_JSON_PATH_IN_ZIP) as f:
            instances_data = json.load(f)
        with zf.open(KEYPOINTS_ANN_JSON_PATH_IN_ZIP) as f:
            keypoints_data = json.load(f)

    if os.environ.get("DATAPIPE_ML_DELETE_COCO_CACHE_AFTER_READ") == "1":
        ann_zip.unlink(missing_ok=True)

    # 3) write per-image JSONs in the requested formats
    write_detection_segmentation_dataset(instances_data)
    write_keypoints_dataset(keypoints_data)

    print("\nDone!")
    print(f"- COCO annotations cache: {coco_annotations_zip_path().resolve()}")
    print(f"- Images: {IMAGES_DIR.resolve()}")
    print(f"- JSONs : {LABELS_DIR.resolve()}")
    print(f"- Keypoints images: {KPS_IMAGES_DIR.resolve()}")
    print(f"- Keypoints JSONs : {KPS_LABELS_DIR.resolve()}")


if __name__ == "__main__":
    main()
