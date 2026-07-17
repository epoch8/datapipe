#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import random
import sys
import time
from collections import defaultdict
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

from tools.sample_data.coco_cache import INSTANCES_JSON, ensure_coco_annotations_zip

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_DIR = SCRIPT_DIR.parent
INPUT_DIR = PROJECT_DIR / "input"
ANNOTATIONS_DIR = INPUT_DIR / "annotations"

COCO_IMG_BASE = "http://images.cocodataset.org/train2017/"

CAT_DOG_CATEGORY_IDS = {17, 18}
DEFAULT_IMAGE_COUNT = 20
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


def load_coco_instances(ann_zip: Path) -> dict:
    with ZipFile(ann_zip, "r") as zf:
        with zf.open(INSTANCES_JSON) as handle:
            return json.load(handle)


def pick_cat_dog_image_ids(instances_data: dict, limit: int) -> list[int]:
    random.seed(RNG_SEED)
    anns_by_image: dict[int, list] = defaultdict(list)
    for ann in instances_data["annotations"]:
        if ann.get("category_id") not in CAT_DOG_CATEGORY_IDS:
            continue
        if ann.get("iscrowd", 0) == 1 or not ann.get("bbox"):
            continue
        anns_by_image[ann["image_id"]].append(ann)

    valid_image_ids = [image_id for image_id, anns in anns_by_image.items() if anns]
    if len(valid_image_ids) < limit:
        raise RuntimeError(f"Need at least {limit} cat/dog images in COCO, found {len(valid_image_ids)}")

    return random.sample(valid_image_ids, limit)


def build_cvat_image_xml(annotations: list[dict], cat_id_to_name: dict[int, str]) -> str:
    lines = ["<image>"]
    for ann in annotations:
        x, y, w, h = ann["bbox"]
        label = cat_id_to_name[ann["category_id"]]
        lines.append(
            f'        <box label="{label}" source="manual" occluded="0" '
            f'xtl="{x:.2f}" ytl="{y:.2f}" xbr="{x + w:.2f}" ybr="{y + h:.2f}" z_order="0">'
        )
        lines.append("        </box>")
    lines.append("</image>")
    return "\n".join(lines)


def download_sample_dataset(*, limit: int = DEFAULT_IMAGE_COUNT) -> list[Path]:
    ann_zip = ensure_coco_annotations_zip()
    instances_data = load_coco_instances(ann_zip)
    image_ids = pick_cat_dog_image_ids(instances_data, limit)
    id_to_img = {img["id"]: img for img in instances_data["images"]}
    cat_id_to_name = {category["id"]: category["name"] for category in instances_data["categories"]}

    anns_by_image: dict[int, list] = defaultdict(list)
    for ann in instances_data["annotations"]:
        if ann["image_id"] in image_ids and ann.get("category_id") in CAT_DOG_CATEGORY_IDS:
            if ann.get("iscrowd", 0) == 1 or not ann.get("bbox"):
                continue
            anns_by_image[ann["image_id"]].append(ann)

    INPUT_DIR.mkdir(parents=True, exist_ok=True)
    ANNOTATIONS_DIR.mkdir(parents=True, exist_ok=True)
    downloaded: list[Path] = []

    print(f"Downloading {limit} COCO cat/dog images to {INPUT_DIR} ...")
    for image_id in tqdm(image_ids):
        file_name = id_to_img[image_id]["file_name"]
        local_path = INPUT_DIR / file_name
        downloaded.append(local_path)
        if not local_path.exists():
            download_file(COCO_IMG_BASE + file_name, local_path, file_name)

        annotation_path = ANNOTATIONS_DIR / f"{local_path.stem}.xml"
        annotation_path.write_text(
            build_cvat_image_xml(anns_by_image[image_id], cat_id_to_name),
            encoding="utf-8",
        )

    print(f"Saved {len(downloaded)} images and annotations to {INPUT_DIR}")
    return downloaded


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download COCO cat/dog sample images and CVAT preannotations (same source as e2e_template)."
    )
    parser.add_argument("--limit", type=int, default=DEFAULT_IMAGE_COUNT, help="Number of COCO images")
    parser.add_argument("--skip-download", action="store_true", help="Reuse files already in input/")
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    if args.skip_download:
        if not any(INPUT_DIR.glob("*.jpg")):
            print(f"No images found in {INPUT_DIR}", file=sys.stderr)
            return 1
        return 0

    download_sample_dataset(limit=args.limit)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
