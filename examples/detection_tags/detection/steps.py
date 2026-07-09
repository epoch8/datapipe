from __future__ import annotations

import os

import fsspec
import numpy as np
import pandas as pd
from datapipe_ml.core.image_data import convert_df_with_bbox_to_df_with_image_data

from coco_demo import CocoDemoSource, darken as demo_darken
from config import (
    LOCAL_IMAGES_DIR,
    input_bucket,
    input_image_url,
    input_storage_options,
)

# --- load step (demo: pulls images/GT from coco_demo; swap that module for real data) -----------
def load_batch(df_request: pd.DataFrame):
    """For each load_request row: pull images + ground truth from the demo source (coco_demo),
    optionally darken (a tagged scenario), upload to object storage, and return (s3_images,
    image__ground_truth, tag, image__tag, image__subset_hint). image_name is the object basename.
    For REAL data, replace coco_demo — this step itself is source-agnostic plumbing."""
    s3_cols = ["image_name", "image_url"]
    gt_cols = ["image_name", "bboxes", "labels"]
    tag_cols = ["tag_id", "tag_description"]
    it_cols = ["image_name", "tag_id"]
    hint_cols = ["image_name", "subset_id"]
    if df_request.empty:
        return (pd.DataFrame(columns=s3_cols), pd.DataFrame(columns=gt_cols),
                pd.DataFrame(columns=tag_cols), pd.DataFrame(columns=it_cols),
                pd.DataFrame(columns=hint_cols))

    source = CocoDemoSource()   # demo data (COCO cat/dog); for real data swap coco_demo.py
    fs = fsspec.filesystem("s3", **input_storage_options())
    bucket = input_bucket()

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
        for fn in source.pool[offset: offset + n]:
            stem, ext = os.path.splitext(fn)
            raw, boxes, labels = source.fetch(fn)
            if darken is not None:
                name = f"{stem}__{tag or 'dark'}{ext}"
                data = demo_darken(raw, darken)
            else:
                name, data = fn, raw
            fs.pipe(f"{bucket}/images/{name}", data)
            s3_rows.append({"image_name": name, "image_url": input_image_url(name)})
            gt_rows.append({"image_name": name, "bboxes": boxes, "labels": labels})
            if tag:
                tag_rows.append({"image_name": name, "tag_id": tag})
                # tag_defs[name] = readable description; note darkening if any
                desc = tag if darken is None else f"{tag} (low-light, gamma {darken})"
                tag_defs[tag] = desc
            if subset:
                hint_rows.append({"image_name": name, "subset_id": subset})

    return (
        pd.DataFrame(s3_rows, columns=s3_cols),
        pd.DataFrame(gt_rows, columns=gt_cols),
        pd.DataFrame([{"tag_id": nm, "tag_description": d}
                      for nm, d in tag_defs.items()], columns=tag_cols),
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
            "calc__support", "calc__TP", "calc__FP", "calc__FN",
            "calc__precision", "calc__recall", "calc__f1_score"]
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
    g["calc__TP"] = tp.astype(int)
    g["calc__FP"] = fp.astype(int)
    g["calc__FN"] = fn.astype(int)
    return g[cols]


# --- FiftyOne (stage=fiftyone) --------------------------------------------------
# Same pattern as examples/e2e_template/image_detection: download images locally,
# register samples, then publish GT / predictions into separate fields of one dataset.


def download_images(
    s3_images_df: pd.DataFrame,
    image__image_path__name: str,
    image__local_image_path__name: str,
) -> pd.DataFrame:
    LOCAL_IMAGES_DIR.mkdir(parents=True, exist_ok=True)
    local_paths = []
    for _, row in s3_images_df.iterrows():
        s3_path = row[image__image_path__name]
        local_path = LOCAL_IMAGES_DIR / os.path.basename(str(s3_path))
        if not local_path.exists():
            with fsspec.open(s3_path, "rb") as f_src:
                local_path.write_bytes(f_src.read())
        local_paths.append(str(local_path.resolve()))
    s3_images_df[image__local_image_path__name] = local_paths
    return s3_images_df[["image_name", image__local_image_path__name]]


def publish_to_fiftyone(
    images_df: pd.DataFrame,
    primary_keys: list[str],
    image__image_path__name: str,
) -> pd.DataFrame:
    return convert_df_with_bbox_to_df_with_image_data(
        df__with_bbox=images_df,
        image__image_path__name=image__image_path__name,
        primary_keys=primary_keys,
    )


def publish_to_fiftyone_ground_truth(
    images_df: pd.DataFrame,
    ground_truth_df: pd.DataFrame,
    subset_df: pd.DataFrame,
    tag_df: pd.DataFrame,
    primary_keys: list[str],
    image__image_path__name: str,
) -> pd.DataFrame:
    df = pd.merge(images_df, ground_truth_df, on=primary_keys, how="left")
    df = pd.merge(df, subset_df, on=primary_keys, how="left")
    if tag_df is not None and not tag_df.empty:
        df = pd.merge(df, tag_df[primary_keys + ["tag_id"]], on=primary_keys, how="left")
    return convert_df_with_bbox_to_df_with_image_data(
        df__with_bbox=df,
        primary_keys=primary_keys,
        image__image_path__name=image__image_path__name,
    )


def _baseline_and_retrained_model_ids(predictions_df: pd.DataFrame) -> tuple[list[str], list[str]]:
    """Earliest-trained model = baseline; the next one = retrained (stable A/B across images)."""
    ids = sorted(predictions_df["detection_model_id"].dropna().unique().tolist())
    return ([ids[0]] if ids else []), (ids[1:2] if len(ids) > 1 else [])


def _publish_predictions_for_models(
    images_df: pd.DataFrame,
    predictions_df: pd.DataFrame,
    model_ids: list[str],
    model_id_sample_field: str,
    primary_keys: list[str],
    image__image_path__name: str,
) -> pd.DataFrame:
    empty = pd.DataFrame(columns=primary_keys + ["image_data"])
    if predictions_df.empty or not model_ids:
        return empty
    pred = predictions_df[predictions_df["detection_model_id"].isin(model_ids)]
    if pred.empty:
        return empty
    df = pd.merge(pred, images_df, on=primary_keys, how="left")
    df[model_id_sample_field] = df["detection_model_id"]
    return convert_df_with_bbox_to_df_with_image_data(
        df__with_bbox=df,
        primary_keys=primary_keys,
        image__image_path__name=image__image_path__name,
    )


def publish_to_fiftyone_predictions_baseline(
    images_df: pd.DataFrame,
    predictions_df: pd.DataFrame,
    primary_keys: list[str],
    image__image_path__name: str,
) -> pd.DataFrame:
    baseline_ids, _ = _baseline_and_retrained_model_ids(predictions_df)
    return _publish_predictions_for_models(
        images_df, predictions_df, baseline_ids, "detection_model_id_a",
        primary_keys, image__image_path__name,
    )


def publish_to_fiftyone_predictions_retrained(
    images_df: pd.DataFrame,
    predictions_df: pd.DataFrame,
    primary_keys: list[str],
    image__image_path__name: str,
) -> pd.DataFrame:
    _, retrained_ids = _baseline_and_retrained_model_ids(predictions_df)
    return _publish_predictions_for_models(
        images_df, predictions_df, retrained_ids, "detection_model_id_b",
        primary_keys, image__image_path__name,
    )
