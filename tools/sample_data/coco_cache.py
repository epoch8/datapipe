from __future__ import annotations

import os
import time
from collections.abc import Callable
from pathlib import Path
from zipfile import BadZipFile, ZipFile

COCO_ANN_ZIP_URL = "http://images.cocodataset.org/annotations/annotations_trainval2017.zip"
INSTANCES_JSON = "annotations/instances_train2017.json"
KEYPOINTS_JSON = "annotations/person_keypoints_train2017.json"
REQUIRED_COCO_ANN_ENTRIES = (INSTANCES_JSON, KEYPOINTS_JSON)
# Official trainval2017 annotations zip is ~241MB; reject truncated downloads early.
MIN_COCO_ANN_ZIP_BYTES = 200 * 2**20


def datapipe_cache_dir() -> Path:
    if override := os.environ.get("DATAPIPE_CACHE_DIR"):
        return Path(override)
    xdg_cache = os.environ.get("XDG_CACHE_HOME")
    base = Path(xdg_cache) if xdg_cache else Path.home() / ".cache"
    return base / "datapipe"


def coco_annotations_zip_path() -> Path:
    return datapipe_cache_dir() / "coco" / "annotations_trainval2017.zip"


def coco_annotations_zip_is_valid(path: Path | None = None) -> bool:
    zip_path = path or coco_annotations_zip_path()
    if not zip_path.is_file():
        return False
    if zip_path.stat().st_size < MIN_COCO_ANN_ZIP_BYTES:
        return False
    try:
        with ZipFile(zip_path, "r") as zf:
            if zf.testzip() is not None:
                return False
            for entry in REQUIRED_COCO_ANN_ENTRIES:
                zf.getinfo(entry)
        return True
    except (BadZipFile, KeyError, OSError):
        return False


def remove_invalid_coco_annotations_zip(path: Path | None = None) -> None:
    zip_path = path or coco_annotations_zip_path()
    if zip_path.exists() and not coco_annotations_zip_is_valid(zip_path):
        zip_path.unlink(missing_ok=True)


def default_download_coco_annotations_zip(dst: Path) -> None:
    import requests
    from tqdm import tqdm

    dst.parent.mkdir(parents=True, exist_ok=True)
    tmp_dst = dst.with_suffix(f"{dst.suffix}.tmp")
    tmp_dst.unlink(missing_ok=True)

    last_error: Exception | None = None
    for attempt in range(1, 6):
        try:
            with requests.get(COCO_ANN_ZIP_URL, timeout=60, stream=True) as response:
                response.raise_for_status()
                total = int(response.headers.get("content-length", 0))
                with open(tmp_dst, "wb") as handle, tqdm(
                    total=total, unit="B", unit_scale=True, desc="annotations"
                ) as bar:
                    for chunk in response.iter_content(8192):
                        if chunk:
                            handle.write(chunk)
                            bar.update(len(chunk))
            tmp_dst.replace(dst)
            return
        except requests.RequestException as exc:
            last_error = exc
            tmp_dst.unlink(missing_ok=True)
            if attempt == 5:
                break
            time.sleep(min(2**attempt, 30))
    assert last_error is not None
    raise last_error


def ensure_coco_annotations_zip(
    download: Callable[[Path], None] | None = None,
) -> Path:
    zip_path = coco_annotations_zip_path()
    if coco_annotations_zip_is_valid(zip_path):
        return zip_path

    remove_invalid_coco_annotations_zip(zip_path)
    print(f"Downloading COCO annotations (~241MB, cached in {zip_path.parent}/)...")
    (download or default_download_coco_annotations_zip)(zip_path)

    if not coco_annotations_zip_is_valid(zip_path):
        zip_path.unlink(missing_ok=True)
        raise RuntimeError(f"Downloaded COCO annotations archive is invalid: {zip_path}")

    return zip_path
