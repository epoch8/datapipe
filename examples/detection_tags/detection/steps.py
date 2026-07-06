from __future__ import annotations

from typing import Iterator

import fsspec
import pandas as pd

from config import INPUT_IMAGES_DIR, input_image_url, input_storage_options


def list_s3_images() -> Iterator[pd.DataFrame]:
    """List input images from object storage; image_name = key relative to INPUT_IMAGES_DIR."""
    fs, root = fsspec.core.url_to_fs(INPUT_IMAGES_DIR, **input_storage_options())
    keys, urls = [], []
    for path in fs.find(root):
        if not path.lower().endswith((".jpg", ".jpeg", ".png", ".webp")):
            continue
        rel_key = path[len(root):].lstrip("/")
        keys.append(rel_key.replace("/", "___"))
        urls.append(input_image_url(rel_key))
    yield pd.DataFrame({"image_name": keys, "image_url": urls})


def split_df_train_val(
    df: pd.DataFrame,
    subset_df: pd.DataFrame,
    primary_keys: list[str],
    val_perc: float = 0.25,
    random_seed: int = 42,
) -> pd.DataFrame:
    """Assign a train/val subset to any image that doesn't have one yet (stable)."""
    df__merged = pd.merge(df, subset_df, on=primary_keys, how="outer")
    df__missing = df__merged[df__merged["subset_id"].isna()].copy()
    df__val = df__missing.sample(frac=val_perc, random_state=random_seed)
    df__missing["subset_id"] = "train"
    df__missing.loc[df__val.index, "subset_id"] = "val"
    return pd.concat([subset_df, df__missing], ignore_index=True)[primary_keys + ["subset_id"]]


def compute_tag_metrics(df_metrics_on_image: pd.DataFrame, df_image_tag: pd.DataFrame) -> pd.DataFrame:
    """Aggregate per-image detection metrics by (model, tag, subset).

    transform_keys=['detection_model_id'] groups one model's whole per-image table into a
    single call, so the aggregation is correct (no cross-chunk splitting). df_image_tag has
    no detection_model_id, so datapipe broadcasts it in full.
    """
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
