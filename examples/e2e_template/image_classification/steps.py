from __future__ import annotations

import json
import os
from typing import Iterator

import fsspec
import pandas as pd
from cv_pipeliner import ImageData

from config import (
    INPUT_IMAGES_DIR,
    LOCAL_IMAGES_DIR,
    SEED_CLASSIFICATION_LABELS_PATH,
    input_image_url,
    input_storage_options,
)


def list_s3_images() -> Iterator[pd.DataFrame]:
    fs, root = fsspec.core.url_to_fs(INPUT_IMAGES_DIR, **input_storage_options())
    keys, urls = [], []
    for path in fs.find(root):
        if not path.lower().endswith((".jpg", ".jpeg", ".png", ".webp")):
            continue
        rel_key = path[len(root) :].lstrip("/")
        keys.append(rel_key.replace("/", "___"))
        urls.append(input_image_url(rel_key))

    yield pd.DataFrame({"image_name": keys, "image_url": urls})


def list_seed_classification_labels() -> Iterator[pd.DataFrame]:
    """COCO-derived labels from ``scripts/seed_sample_data.py`` (optional pre-labels)."""
    if not SEED_CLASSIFICATION_LABELS_PATH.exists():
        yield pd.DataFrame(columns=["image_name", "label"])
        return
    payload = json.loads(SEED_CLASSIFICATION_LABELS_PATH.read_text(encoding="utf-8"))
    rows = [{"image_name": name, "label": label} for name, label in payload.items()]
    yield pd.DataFrame(rows, columns=["image_name", "label"])


def resolve_best_classification_model(
    models_df: pd.DataFrame,
    best_model_df: pd.DataFrame,
    model_id_column: str,
) -> pd.DataFrame:
    if not best_model_df.empty:
        return best_model_df[[model_id_column]]
    if models_df.empty:
        return pd.DataFrame(columns=[model_id_column])
    return models_df.iloc[[0]][[model_id_column]]


def prediction_to_ls_prediction(
    df_pred: pd.DataFrame,
    primary_keys: list[str],
    model_keys: list[str],
) -> pd.DataFrame:
    """Convert classification inference rows into Label Studio Choices predictions."""
    output_keys = list(dict.fromkeys(primary_keys + model_keys))
    if df_pred.empty:
        return pd.DataFrame(columns=output_keys + ["prediction"])

    df = df_pred.copy()
    if "prediction__top_n" in df.columns:
        df = df[df["prediction__top_n"].astype(str) == "1"]
    if df.empty:
        return pd.DataFrame(columns=output_keys + ["prediction"])

    df["prediction"] = df["label"].apply(
        lambda label: {
            "result": [
                {
                    "from_name": "label",
                    "to_name": "image",
                    "type": "choices",
                    "value": {"choices": [str(label)]},
                }
            ]
        }
    )
    return df[output_keys + ["prediction"]]


def parse_annotations_from_label_studio(df: pd.DataFrame) -> pd.DataFrame:
    records = []
    for _, row in df.iterrows():
        annotations = row.get("annotations", [])
        if not annotations:
            continue
        result = annotations[-1].get("result") or []
        label = None
        for item in result:
            if item.get("type") != "choices":
                continue
            choices = (item.get("value") or {}).get("choices") or []
            if choices:
                label = str(choices[0])
                break
        if label is None:
            continue
        records.append({"image_name": row["image_name"], "label": label})
    return pd.DataFrame(records, columns=["image_name", "label"])


def fill_ground_truth_from_seed(
    seed_df: pd.DataFrame,
    gt_df: pd.DataFrame,
    primary_keys: list[str],
) -> pd.DataFrame:
    """Fill missing GT from COCO seed labels; keep Label Studio annotations when present."""
    if seed_df.empty:
        return gt_df if not gt_df.empty else pd.DataFrame(columns=primary_keys + ["label"])
    if gt_df.empty:
        return seed_df[primary_keys + ["label"]]
    missing = seed_df.merge(gt_df[primary_keys], on=primary_keys, how="left", indicator=True)
    missing = missing[missing["_merge"] == "left_only"][primary_keys + ["label"]]
    return pd.concat([gt_df, missing], ignore_index=True)[primary_keys + ["label"]]


def split_df_train_val(
    df: pd.DataFrame,
    subset_df: pd.DataFrame,
    primary_keys: list[str],
    val_perc: float = 0.25,
    random_seed: int = 42,
) -> pd.DataFrame:
    df__merged = pd.merge(df, subset_df, on=primary_keys, how="outer")
    df__missing = df__merged[df__merged["subset_id"].isna()].copy()
    df__val = df__missing.sample(frac=val_perc, random_state=random_seed)
    df__missing["subset_id"] = "train"
    df__missing.loc[df__val.index, "subset_id"] = "val"
    return pd.concat([subset_df, df__missing], ignore_index=True)[primary_keys + ["subset_id"]]


def download_images(
    s3_images_df: pd.DataFrame,
    image__image_path__name: str,
    image__local_image_path__name: str,
) -> pd.DataFrame:
    output_dir = LOCAL_IMAGES_DIR
    output_dir.mkdir(parents=True, exist_ok=True)
    local_paths = []
    for _, row in s3_images_df.iterrows():
        s3_path = row[image__image_path__name]
        local_path = output_dir / os.path.basename(str(s3_path))
        if not local_path.exists():
            with fsspec.open(s3_path, "rb") as f_src:
                local_path.write_bytes(f_src.read())
        local_paths.append(str(local_path.resolve()))
    s3_images_df[image__local_image_path__name] = local_paths
    return s3_images_df[["image_name", image__local_image_path__name]]


def publish_to_fiftyone(images_df: pd.DataFrame, primary_keys: list[str], image__image_path__name: str) -> pd.DataFrame:
    images_df = images_df.copy()
    images_df["image_data"] = images_df[image__image_path__name].apply(
        lambda path: ImageData(image_path=str(path))
    )
    return images_df[primary_keys + ["image_data"]]


def publish_to_fiftyone_ground_truth(
    images_df: pd.DataFrame,
    ground_truth_df: pd.DataFrame,
    subset_df: pd.DataFrame,
    primary_keys: list[str],
    image__image_path__name: str,
) -> pd.DataFrame:
    images_df = pd.merge(images_df, ground_truth_df, on=primary_keys)
    images_df = pd.merge(images_df, subset_df, on=primary_keys)
    records = []
    for _, row in images_df.iterrows():
        image_data = ImageData(image_path=str(row[image__image_path__name]), label=str(row["label"]))
        image_data.additional_info["subset_id"] = row["subset_id"]
        records.append({**{k: row[k] for k in primary_keys}, "image_data": image_data})
    return pd.DataFrame(records, columns=primary_keys + ["image_data"])


def publish_to_fiftyone_predictions_from_best_model(
    images_df: pd.DataFrame,
    predictions_df: pd.DataFrame,
    best_classification_model_df: pd.DataFrame,
    primary_keys: list[str],
    model_keys: list[str],
    image__image_path__name: str,
) -> pd.DataFrame:
    df = pd.merge(predictions_df, images_df, on=primary_keys)
    df = pd.merge(df, best_classification_model_df, on=model_keys)
    if "prediction__top_n" in df.columns:
        df = df[df["prediction__top_n"].astype(str) == "1"]
    records = []
    for _, row in df.iterrows():
        image_data = ImageData(image_path=str(row[image__image_path__name]), label=str(row["label"]))
        image_data.additional_info["classification_model_id"] = row["classification_model_id"]
        records.append({**{k: row[k] for k in primary_keys}, "image_data": image_data})
    return pd.DataFrame(records, columns=primary_keys + ["image_data"])
