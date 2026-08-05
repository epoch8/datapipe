"""DEMO DATA SYNTHESIS — COCO cat/dog (replace this module for real data).

Everything here exists only to fabricate the demo dataset: download the COCO cat/dog subset, inject
ground truth, and darken images to fake a low-light "night" scenario. None of it is part of the tag
pipeline itself — for REAL data, drop this module and feed the `load` step from your own source of
(image bytes, boxes, labels). The pipeline (steps.py / app.py) doesn't care where images come from.
"""
from __future__ import annotations

import io
import json
import os
import random
import time
import zipfile
from pathlib import Path
from typing import Optional

import numpy as np
import requests
from PIL import Image

from config import COCO_CAT_IDS

COCO_IMG_BASE = "http://images.cocodataset.org/train2017/"
COCO_ANN_URL = "http://images.cocodataset.org/annotations/annotations_trainval2017.zip"
CACHE = Path(os.environ.get("DATAPIPE_TAGS_CACHE_DIR", "/tmp/datapipe-tags-cache"))
RNG_SEED = 1234


def _get(url: str, attempts: int = 4) -> requests.Response:
    last: Optional[Exception] = None
    for a in range(attempts):
        try:
            r = requests.get(url, timeout=60, stream=True)
            r.raise_for_status()
            return r
        except requests.RequestException as e:
            last = e
            time.sleep(min(2 ** a, 20))
    assert last is not None
    raise last


def _coco_annotations_by_image() -> tuple[dict, dict]:
    """Return (id_to_image_meta, {image_id: [cat/dog annotations]}), cached on disk."""
    CACHE.mkdir(parents=True, exist_ok=True)
    zip_path = CACHE / "annotations_trainval2017.zip"
    if not zip_path.exists() or zip_path.stat().st_size < 1_000_000:
        with _get(COCO_ANN_URL) as r, open(zip_path, "wb") as f:
            for chunk in r.iter_content(1 << 20):
                f.write(chunk)
    with zipfile.ZipFile(zip_path) as zf, zf.open("annotations/instances_train2017.json") as h:
        inst = json.load(h)
    id_to_img = {im["id"]: im for im in inst["images"]}
    anns: dict[int, list] = {}
    for a in inst["annotations"]:
        if a.get("category_id") in COCO_CAT_IDS and not a.get("iscrowd", 0) and a.get("bbox"):
            anns.setdefault(a["image_id"], []).append(a)
    return id_to_img, anns


def darken(raw: bytes, gamma: float) -> bytes:
    """Gamma-darken JPEG bytes to fake a low-light scene (gamma < 1 darkens). Demo-only."""
    a = np.asarray(Image.open(io.BytesIO(raw)).convert("RGB")).astype(np.float32) / 255.0
    out = Image.fromarray((np.power(a, 1.0 / gamma) * 255).clip(0, 255).astype(np.uint8))
    buf = io.BytesIO()
    out.save(buf, format="JPEG", quality=92)
    return buf.getvalue()


class CocoDemoSource:
    """Deterministic shuffled pool of COCO cat/dog filenames + per-file (bytes, boxes, labels).

    Canonical pool order = filenames sorted, then a fixed seeded shuffle (RNG_SEED), so `offset`/`n`
    slices are reproducible across runs and machines. Prefers a pre-staged cache
    (`$DATAPIPE_TAGS_CACHE_DIR/gt.json` + `images/<fn>`, built by scripts/build_cache.py, which persists
    the first N of that canonical order); loads then read the cached key order AS-IS. Falls back to
    downloading from COCO in the same canonical order, so cache and download select identically.
    """

    def __init__(self) -> None:
        cache_gt_path = CACHE / "gt.json"
        self._use_cache = cache_gt_path.exists() and (CACHE / "images").is_dir()
        if self._use_cache:
            # The cache is pre-shuffled at build time (build_cache.py bakes in the canonical order),
            # so read the persisted key order AS-IS. Re-sorting/re-shuffling here would diverge from
            # the download path's selection for the same offset/n slices.
            self._cache_gt = json.loads(cache_gt_path.read_text())
            pool = list(self._cache_gt.keys())
        else:
            # Canonical order: sort by FILENAME (machine-independent) then a fixed seeded shuffle.
            # build_cache.py persists the first N of THIS order, so cache == download for any offset.
            self._id_to_img, self._anns = _coco_annotations_by_image()
            self._fn_to_id = {self._id_to_img[i]["file_name"]: i for i in self._anns.keys()}
            pool = sorted(self._fn_to_id.keys())
            random.seed(RNG_SEED)
            random.shuffle(pool)
        self.pool = pool

    def fetch(self, fn: str) -> tuple[bytes, list, list]:
        """Return (image_bytes, bboxes[[x1,y1,x2,y2]], labels[lowercase]) for a pool filename."""
        if self._use_cache:
            data = (CACHE / "images" / fn).read_bytes()
            g = self._cache_gt[fn]
            return data, g["bboxes"], g["labels"]
        raw = _get(COCO_IMG_BASE + fn).content
        anns = self._anns[self._fn_to_id[fn]]
        boxes = [[int(a["bbox"][0]), int(a["bbox"][1]),
                  int(a["bbox"][0] + a["bbox"][2]), int(a["bbox"][1] + a["bbox"][3])] for a in anns]
        labels = [COCO_CAT_IDS[a["category_id"]] for a in anns]
        return raw, boxes, labels
