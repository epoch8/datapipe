#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import random
import sys
import time
from pathlib import Path
from zipfile import ZipFile

import fsspec
import requests
from tqdm import tqdm

for parent in Path(__file__).resolve().parents:
    if (parent / "tools" / "sample_data" / "coco_cache.py").exists():
        sys.path.insert(0, str(parent))
        break
else:
    raise RuntimeError("Cannot find datapipe workspace root (expected tools/sample_data/coco_cache.py)")

from tools.sample_data.coco_cache import INSTANCES_JSON, KEYPOINTS_JSON, ensure_coco_annotations_zip

SCRIPT_DIR = Path(__file__).resolve().parent
E2E_DIR = SCRIPT_DIR.parent
SAMPLE_DIR = E2E_DIR / "sample_data" / "images"
MODELS_DIR = E2E_DIR / "sample_data" / "models"
YOLO_ASSETS_RELEASE = "https://github.com/ultralytics/assets/releases/download/v8.4.0"
E2E_YOLO_WEIGHTS = {
    "yolo11n.pt": f"{YOLO_ASSETS_RELEASE}/yolo11n.pt",
    "yolo11n-pose.pt": f"{YOLO_ASSETS_RELEASE}/yolo11n-pose.pt",
}

COCO_IMG_BASE = "http://images.cocodataset.org/train2017/"

CAT_DOG_CATEGORY_IDS = {17, 18}  # cat, dog
RNG_SEED = 1234


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


def download_file(url: str, dst: Path, desc: str) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    tmp_dst = dst.with_suffix(f"{dst.suffix}.tmp")
    tmp_dst.unlink(missing_ok=True)
    with get_with_retries(url) as response:
        total = int(response.headers.get("content-length", 0))
        with open(tmp_dst, "wb") as handle, tqdm(total=total, unit="B", unit_scale=True, desc=desc) as bar:
            for chunk in response.iter_content(8192):
                if chunk:
                    handle.write(chunk)
                    bar.update(len(chunk))
    tmp_dst.replace(dst)


def load_coco_json(ann_zip: Path, path_in_zip: str) -> dict:
    with ZipFile(ann_zip, "r") as zf:
        with zf.open(path_in_zip) as handle:
            return json.load(handle)


def pick_image_ids(instances_data: dict, keypoints_data: dict, detection_limit: int, keypoints_limit: int) -> list[int]:
    random.seed(RNG_SEED)
    id_to_img = {img["id"]: img for img in instances_data["images"]}

    cat_dog_by_image: dict[int, list] = {}
    for ann in instances_data["annotations"]:
        if ann.get("category_id") not in CAT_DOG_CATEGORY_IDS:
            continue
        if ann.get("iscrowd", 0) == 1 or not ann.get("bbox"):
            continue
        cat_dog_by_image.setdefault(ann["image_id"], []).append(ann)

    person_by_image: dict[int, list] = {}
    for ann in keypoints_data["annotations"]:
        if ann.get("iscrowd", 0) == 1 or not ann.get("bbox") or not ann.get("keypoints"):
            continue
        if ann.get("num_keypoints", 0) <= 0:
            continue
        person_by_image.setdefault(ann["image_id"], []).append(ann)

    cat_dog_ids = [iid for iid, anns in cat_dog_by_image.items() if anns]
    person_ids = [iid for iid, anns in person_by_image.items() if anns]
    if len(cat_dog_ids) < detection_limit:
        raise RuntimeError(f"Need at least {detection_limit} cat/dog images in COCO, found {len(cat_dog_ids)}")
    if len(person_ids) < keypoints_limit:
        raise RuntimeError(f"Need at least {keypoints_limit} person keypoint images in COCO, found {len(person_ids)}")

    selected = list(dict.fromkeys(random.sample(cat_dog_ids, detection_limit) + random.sample(person_ids, keypoints_limit)))
    missing = [iid for iid in selected if iid not in id_to_img]
    if missing:
        raise RuntimeError(f"Missing COCO image metadata for ids: {missing[:3]}")
    return selected


def download_images(image_ids: list[int], instances_data: dict) -> list[Path]:
    id_to_img = {img["id"]: img for img in instances_data["images"]}
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    downloaded: list[Path] = []

    print(f"Downloading {len(image_ids)} COCO train2017 images...")
    for image_id in tqdm(image_ids):
        file_name = id_to_img[image_id]["file_name"]
        local_path = SAMPLE_DIR / file_name
        downloaded.append(local_path)
        if local_path.exists():
            continue
        url = COCO_IMG_BASE + file_name
        download_file(url, local_path, file_name)
    return downloaded


def s3_storage_options() -> dict:
    client_kwargs: dict = {"region_name": os.environ.get("AWS_REGION", "us-east-1")}
    endpoint_url = os.environ.get("S3_ENDPOINT_URL")
    if endpoint_url:
        client_kwargs["endpoint_url"] = endpoint_url
    return {
        "key": os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin"),
        "secret": os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin"),
        "client_kwargs": client_kwargs,
    }


def ensure_yolo_weights() -> None:
    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Ensuring YOLO weights in {MODELS_DIR} ...")
    for filename, url in E2E_YOLO_WEIGHTS.items():
        dst = MODELS_DIR / filename
        if dst.exists():
            continue
        download_file(url, dst, filename)


def datapipe_e2e_dir() -> str:
    return os.environ.get("DATAPIPE_E2E_DIR", "s3://datapipe-e2e").rstrip("/")


def _is_cloud_path(path: str) -> bool:
    protocol, _ = fsspec.core.split_protocol(path)
    return protocol not in (None, "file")


def images_root() -> str:
    return f"{datapipe_e2e_dir()}/images"


def upload_images(local_paths: list[Path]) -> None:
    dst_root = images_root()
    options = s3_storage_options() if _is_cloud_path(dst_root) else {}
    fs, root = fsspec.core.url_to_fs(dst_root, **options)
    if not _is_cloud_path(dst_root):
        fs.makedirs(root, exist_ok=True)

    print(f"Uploading {len(local_paths)} images to {dst_root}/ ...")
    for local_path in tqdm(local_paths):
        remote_key = f"{root}/{local_path.name}"
        if fs.exists(remote_key):
            continue
        fs.put(str(local_path), remote_key)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download sample COCO images and upload them to local MinIO.")
    parser.add_argument("--detection-limit", type=int, default=120, help="Number of cat/dog images")
    parser.add_argument("--keypoints-limit", type=int, default=10, help="Number of person keypoint images")
    parser.add_argument("--skip-download", action="store_true", help="Only upload files already in sample_data/")
    parser.add_argument("--skip-upload", action="store_true", help="Only download images locally")
    parser.add_argument("--skip-models", action="store_true", help="Do not download YOLO smoke weights")
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    if not args.skip_models:
        ensure_yolo_weights()

    if args.skip_download:
        local_paths = sorted(SAMPLE_DIR.glob("*.jpg"))
        if not local_paths:
            print(f"No images found in {SAMPLE_DIR}", file=sys.stderr)
            return 1
    else:
        ann_zip = ensure_coco_annotations_zip()
        instances_data = load_coco_json(ann_zip, INSTANCES_JSON)
        keypoints_data = load_coco_json(ann_zip, KEYPOINTS_JSON)
        image_ids = pick_image_ids(
            instances_data,
            keypoints_data,
            detection_limit=args.detection_limit,
            keypoints_limit=args.keypoints_limit,
        )
        local_paths = download_images(image_ids, instances_data)
        print(f"Saved {len(local_paths)} images to {SAMPLE_DIR}")

    if not args.skip_upload:
        upload_images(local_paths)
        print(f"Uploaded to {images_root()}/")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
