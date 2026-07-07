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
from datapipe_ml.core.image_data import convert_df_with_bbox_to_df_with_image_data
from PIL import Image

from config import (
    COCO_CAT_IDS,
    DBCONN,
    LOCAL_IMAGES_DIR,
    input_bucket,
    input_image_url,
    input_storage_options,
    tag_id_for,
    tag_name_for,
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
    tag_cols = ["tag_id", "tag_name", "tag_description"]
    it_cols = ["image_name", "tag_id"]
    hint_cols = ["image_name", "subset_id"]
    if df_request.empty:
        return (pd.DataFrame(columns=s3_cols), pd.DataFrame(columns=gt_cols),
                pd.DataFrame(columns=tag_cols), pd.DataFrame(columns=it_cols),
                pd.DataFrame(columns=hint_cols))

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

    s3_rows, gt_rows, tag_rows, tag_defs, hint_rows = [], [], [], {}, []
    for _, req in df_request.iterrows():
        n = int(req["n"])
        offset = int(req.get("offset") or 0)
        # NB: when several requests load in one batch, a NULL in a numeric column arrives as NaN
        # (not None) because pandas upcasts the column — so guard with pd.isna, else a base batch
        # with darken=NULL would be "darkened" with gamma=NaN and produce garbage images.
        tag = req.get("tag")
        tag = None if pd.isna(tag) or str(tag) == "" else str(tag)
        subset = req.get("subset")
        subset = None if pd.isna(subset) or str(subset) == "" else str(subset)
        darken = req.get("darken")
        darken = None if pd.isna(darken) or str(darken) == "" else float(darken)
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
                tid = tag_id_for(tag)
                tag_rows.append({"image_name": name, "tag_id": tid})
                # tag_defs[name] = (numeric id, readable description); note darkening if any
                desc = tag if darken is None else f"{tag} (low-light, gamma {darken})"
                tag_defs[tag] = (tid, desc)
            if subset:
                hint_rows.append({"image_name": name, "subset_id": subset})

    return (
        pd.DataFrame(s3_rows, columns=s3_cols),
        pd.DataFrame(gt_rows, columns=gt_cols),
        pd.DataFrame([{"tag_id": tid, "tag_name": nm, "tag_description": d}
                      for nm, (tid, d) in tag_defs.items()], columns=tag_cols),
        pd.DataFrame(tag_rows, columns=it_cols),
        pd.DataFrame(hint_rows, columns=hint_cols),
    )


# --- train/val split ------------------------------------------------------------
def split_df_train_val(df: pd.DataFrame, subset_df: pd.DataFrame, hint_df: pd.DataFrame,
                       primary_keys: list[str], val_perc: float = 0.25,
                       random_seed: int = 42) -> pd.DataFrame:
    """Assign a subset to every image that doesn't have one yet. An image whose subset the load
    step pinned via ``image__subset_hint`` (batches loaded with ``--subset``) keeps that pinned
    value; the rest are split train/val at random. Honoring the hint is what lets the demo FREEZE
    val — load base-val + night-val up front (subset=val) and their assignment never changes when
    train batches are added later, so a model's val metrics are stable across retrainings."""
    df__merged = pd.merge(df, subset_df, on=primary_keys, how="outer")
    df__missing = df__merged[df__merged["subset_id"].isna()][primary_keys].copy()
    if hint_df is not None and not hint_df.empty:
        hint = hint_df[primary_keys + ["subset_id"]].drop_duplicates(subset=primary_keys)
        df__missing = df__missing.merge(hint, on=primary_keys, how="left")
    else:
        df__missing["subset_id"] = np.nan
    df__missing = df__missing.reset_index(drop=True)
    unpinned = df__missing[df__missing["subset_id"].isna()]
    df__val = unpinned.sample(frac=val_perc, random_state=random_seed)
    df__missing.loc[unpinned.index, "subset_id"] = "train"
    df__missing.loc[df__val.index, "subset_id"] = "val"
    return pd.concat([subset_df, df__missing], ignore_index=True)[primary_keys + ["subset_id"]]


# --- tag metrics ----------------------------------------------------------------
def compute_tag_metrics(df_metrics_on_image: pd.DataFrame, df_image_tag: pd.DataFrame) -> pd.DataFrame:
    cols = ["detection_model_id", "tag_id", "subset_id", "calc__images_support",
            "calc__support", "calc__precision", "calc__recall", "calc__f1_score"]
    if df_metrics_on_image.empty or df_image_tag.empty:
        return pd.DataFrame(columns=cols)
    m = df_metrics_on_image.merge(df_image_tag[["image_name", "tag_id"]], on="image_name")
    if m.empty:
        return pd.DataFrame(columns=cols)
    g = (m.groupby(["detection_model_id", "tag_id", "subset_id"], as_index=False)
           .agg(images_support=("image_name", "count"),
                tp=("calc__TP", "sum"), fp=("calc__FP", "sum"), fn=("calc__FN", "sum")))
    tp, fp, fn = g["tp"], g["fp"], g["fn"]
    g["calc__precision"] = (tp / (tp + fp)).where((tp + fp) > 0, 0.0)
    g["calc__recall"] = (tp / (tp + fn)).where((tp + fn) > 0, 0.0)
    p, r = g["calc__precision"], g["calc__recall"]
    g["calc__f1_score"] = (2 * p * r / (p + r)).where((p + r) > 0, 0.0)
    g["calc__support"] = (tp + fn).astype(int)
    g["calc__images_support"] = g["images_support"].astype(int)
    return g[cols]


# --- FiftyOne (stage=fiftyone) --------------------------------------------------
# Same pattern as examples/e2e_template/image_detection: download images locally,
# then publish bbox rows into a FiftyOne dataset via FiftyOneImagesDataTableStore.
def download_images(s3_images_df: pd.DataFrame, image__image_path__name: str,
                    image__local_image_path__name: str) -> pd.DataFrame:
    LOCAL_IMAGES_DIR.mkdir(parents=True, exist_ok=True)
    local_paths = []
    for _, row in s3_images_df.iterrows():
        s3_path = row[image__image_path__name]
        local_path = LOCAL_IMAGES_DIR / os.path.basename(str(s3_path))
        if not local_path.exists():
            # image_url is the public HTTP URL of the MinIO object (anonymous download), so open it
            # plainly — passing S3 storage options here would hit the http filesystem with s3 kwargs.
            with fsspec.open(s3_path, "rb") as f_src:
                local_path.write_bytes(f_src.read())
        local_paths.append(str(local_path.resolve()))
    s3_images_df[image__local_image_path__name] = local_paths
    return s3_images_df[["image_name", image__local_image_path__name]]


def publish_gt_to_fiftyone(images_df: pd.DataFrame, gt_df: pd.DataFrame, subset_df: pd.DataFrame,
                           tag_df: pd.DataFrame, primary_keys: list[str],
                           image__image_path__name: str) -> pd.DataFrame:
    """Publish ground-truth boxes and attach per-sample subset/tag (so you can filter tag=night)."""
    pk = primary_keys[0]
    df = convert_df_with_bbox_to_df_with_image_data(
        df__with_bbox=pd.merge(gt_df, images_df, on=pk),
        primary_keys=primary_keys,
        image__image_path__name=image__image_path__name,
    )
    subset_by_image = (subset_df.drop_duplicates(subset=[pk]).set_index(pk)["subset_id"].to_dict()
                       if not subset_df.empty else {})
    tagid_by_image = (tag_df.drop_duplicates(subset=[pk]).set_index(pk)["tag_id"].to_dict()
                      if not tag_df.empty else {})
    for _, row in df.iterrows():
        tid = tagid_by_image.get(row[pk])
        # FiftyOne sample fields must be strings; write the tag NAME (not the numeric id)
        row["image_data"].additional_info = {
            "subset": subset_by_image.get(row[pk]) or "none",
            "tag": tag_name_for(tid) if tid is not None else "none",
        }
    return df


def publish_predictions_to_fiftyone(images_df: pd.DataFrame, predictions_df: pd.DataFrame,
                                    slot: str, primary_keys: list[str],
                                    image__image_path__name: str) -> pd.DataFrame:
    """Publish one model's predictions into the slot's FiftyOne field. slot='model_a' is the
    earliest-trained model (baseline), 'model_b' the next (retrained). Model ids are
    timestamp-prefixed, so sorting them yields a stable A/B assignment across all images."""
    pk = primary_keys[0]
    empty = pd.DataFrame(columns=primary_keys + ["image_data"])
    if predictions_df.empty:
        return empty
    ids = sorted(predictions_df["detection_model_id"].dropna().unique().tolist())
    if not ids:
        return empty
    slot_of = {mid: ("model_a" if i == 0 else "model_b") for i, mid in enumerate(ids)}
    target_ids = [mid for mid, s in slot_of.items() if s == slot]
    pred = predictions_df[predictions_df["detection_model_id"].isin(target_ids)]
    if pred.empty:
        return empty
    return convert_df_with_bbox_to_df_with_image_data(
        df__with_bbox=pd.merge(pred, images_df, on=pk),
        primary_keys=primary_keys,
        image__image_path__name=image__image_path__name,
    )
