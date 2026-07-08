from __future__ import annotations

import io
import json
import os
import random
import time
import zipfile
from pathlib import Path
from typing import Optional

import fsspec
import numpy as np
import pandas as pd
import requests
from PIL import Image

from config import (
    COCO_CAT_IDS,
    DBCONN,
    input_bucket,
    input_image_url,
    input_storage_options,
)

COCO_IMG_BASE = "http://images.cocodataset.org/train2017/"
COCO_ANN_URL = "http://images.cocodataset.org/annotations/annotations_trainval2017.zip"
CACHE = Path(os.environ.get("DATAPIPE_TAGS_CACHE_DIR", "/tmp/datapipe-tags-cache"))
RNG_SEED = 1234


# --- COCO helpers ---------------------------------------------------------------
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


def _darken(im: Image.Image, gamma: float) -> Image.Image:
    a = np.asarray(im.convert("RGB")).astype(np.float32) / 255.0
    return Image.fromarray((np.power(a, 1.0 / gamma) * 255).clip(0, 255).astype(np.uint8))


# --- load step ------------------------------------------------------------------
def load_batch(df_request: pd.DataFrame):
    """For each load_request row: download COCO cat/dog images, upload to object storage, and
    return (s3_images, image__ground_truth, tag, image__tag). Ground truth uses lowercase COCO
    labels; image_name is the object basename (what a listing would produce)."""
    s3_cols = ["image_name", "image_url"]
    gt_cols = ["image_name", "bboxes", "labels"]
    tag_cols = ["tag_id", "name"]
    it_cols = ["image_name", "tag_id"]
    if df_request.empty:
        return (pd.DataFrame(columns=s3_cols), pd.DataFrame(columns=gt_cols),
                pd.DataFrame(columns=tag_cols), pd.DataFrame(columns=it_cols))

    # Prefer a pre-staged local cache (fast, no COCO): CACHE/images/<fn> + CACHE/gt.json.
    # Download the cache once on a fast machine and copy it here — avoids per-image COCO fetches.
    cache_gt_path = CACHE / "gt.json"
    use_cache = cache_gt_path.exists() and (CACHE / "images").is_dir()
    if use_cache:
        cache_gt = json.loads(cache_gt_path.read_text())
        pool = sorted(cache_gt.keys())  # deterministic order
        random.seed(RNG_SEED)
        random.shuffle(pool)
        anns = None
    else:
        id_to_img, anns = _coco_annotations_by_image()
        by_id = sorted(anns.keys())
        random.seed(RNG_SEED)
        random.shuffle(by_id)
        pool = [id_to_img[i]["file_name"] for i in by_id]  # file names, same order semantics

    fs = fsspec.filesystem("s3", **input_storage_options())
    bucket = input_bucket()

    def raw_bytes_and_gt(fn: str):
        if use_cache:
            data = (CACHE / "images" / fn).read_bytes()
            g = cache_gt[fn]
            return data, g["bboxes"], g["labels"]
        # COCO fallback
        # map filename back to id via anns lookup (filename -> id): rebuild once
        raw = _get(COCO_IMG_BASE + fn).content
        iid = _fn_to_id[fn]
        boxes = [[int(a["bbox"][0]), int(a["bbox"][1]),
                  int(a["bbox"][0] + a["bbox"][2]), int(a["bbox"][1] + a["bbox"][3])] for a in anns[iid]]
        labels = [COCO_CAT_IDS[a["category_id"]] for a in anns[iid]]
        return raw, boxes, labels

    if not use_cache:
        _fn_to_id = {id_to_img[i]["file_name"]: i for i in anns.keys()}

    s3_rows, gt_rows, tag_rows, tag_defs = [], [], [], {}
    for _, req in df_request.iterrows():
        n = int(req["n"])
        offset = int(req.get("offset") or 0)
        tag = req.get("tag") or None
        darken = req.get("darken")
        darken = float(darken) if darken is not None and str(darken) != "" else None
        for fn in pool[offset: offset + n]:
            stem, ext = os.path.splitext(fn)
            raw, boxes, labels = raw_bytes_and_gt(fn)
            if darken is not None:
                name = f"{stem}__{tag or 'dark'}{ext}"
                buf = io.BytesIO()
                _darken(Image.open(io.BytesIO(raw)), darken).save(buf, format="JPEG", quality=92)
                data = buf.getvalue()
            else:
                name, data = fn, raw
            fs.pipe(f"{bucket}/images/{name}", data)
            s3_rows.append({"image_name": name, "image_url": input_image_url(name)})
            gt_rows.append({"image_name": name, "bboxes": boxes, "labels": labels})
            if tag:
                tag_rows.append({"image_name": name, "tag_id": tag})
                tag_defs[tag] = tag

    return (
        pd.DataFrame(s3_rows, columns=s3_cols),
        pd.DataFrame(gt_rows, columns=gt_cols),
        pd.DataFrame([{"tag_id": t, "name": n} for t, n in tag_defs.items()], columns=tag_cols),
        pd.DataFrame(tag_rows, columns=it_cols),
    )


# --- train/val split ------------------------------------------------------------
def split_df_train_val(df: pd.DataFrame, subset_df: pd.DataFrame, primary_keys: list[str],
                       val_perc: float = 0.25, random_seed: int = 42) -> pd.DataFrame:
    df__merged = pd.merge(df, subset_df, on=primary_keys, how="outer")
    df__missing = df__merged[df__merged["subset_id"].isna()].copy()
    df__val = df__missing.sample(frac=val_perc, random_state=random_seed)
    df__missing["subset_id"] = "train"
    df__missing.loc[df__val.index, "subset_id"] = "val"
    return pd.concat([subset_df, df__missing], ignore_index=True)[primary_keys + ["subset_id"]]
