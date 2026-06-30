from __future__ import annotations

import os
from typing import Iterator

import fsspec
import numpy as np
import pandas as pd
from cv_pipeliner import BboxData, ImageData
from cv_pipeliner.utils.label_studio import convert_annotation_to_image_data, convert_image_data_to_annotation
from datapipe.datatable import DataStore
from datapipe_ml.core.image_data import convert_df_with_bbox_to_df_with_image_data
from datapipe_ml.tasks.keypoints.inference import keypoints_inference

from config import (
    CLASSES_TO_KEEP,
    KEYPOINTS_LABELS,
    KEYPOINTS_MODEL_CONFIG,
    LOCAL_IMAGES_DIR,
    INPUT_IMAGES_DIR,
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


def list_keypoints_models() -> Iterator[pd.DataFrame]:
    yield pd.DataFrame([KEYPOINTS_MODEL_CONFIG])


def keypoints_to_ls_prediction(
    df_keypoints: pd.DataFrame,
    df_image: pd.DataFrame,
    image__image_path__name: str,
    hide_bboxes: bool = False,
) -> pd.DataFrame:
    image_name_to_path = dict(zip(df_image["image_name"], df_image[image__image_path__name]))
    records = []
    for model_id, model_df in df_keypoints.groupby("keypoints_model_id"):
        for image_name, group in model_df.groupby("image_name"):
            bboxes_data = []
            for _, row in group.iterrows():
                bboxes = row.get("bboxes", []) or []
                keypoints = row.get("keypoints", []) or []
                labels = row.get("labels", []) or []
                for idx in range(min(len(bboxes), len(labels))):
                    label = str(labels[idx]).lower()
                    if label not in CLASSES_TO_KEEP:
                        continue
                    bbox = bboxes[idx]
                    bbox_keypoints = keypoints[idx] if idx < len(keypoints) and keypoints[idx] is not None else []
                    bboxes_data.append(
                        BboxData(
                            xmin=int(bbox[0]),
                            ymin=int(bbox[1]),
                            xmax=int(bbox[2]),
                            ymax=int(bbox[3]),
                            keypoints=np.array(bbox_keypoints).reshape(-1, 2),
                            label=label,
                        )
                    )
            annotations = convert_image_data_to_annotation(
                image_data=ImageData(image_path=image_name_to_path[image_name], bboxes_data=bboxes_data),
                to_name="image",
                bboxes_from_name="bbox",
                keypoints_from_name="kp",
                keypoints_labels=KEYPOINTS_LABELS,
            )
            if hide_bboxes:
                for annotation in annotations:
                    if annotation.get("type") == "rectanglelabels":
                        annotation["hidden"] = True
            records.append({"image_name": image_name, "prediction": {"result": annotations}, "keypoints_model_id": model_id})
    return pd.DataFrame(records, columns=["image_name", "prediction", "keypoints_model_id"])


def parse_annotations_from_label_studio(df: pd.DataFrame) -> pd.DataFrame:
    records = []
    for _, row in df.iterrows():
        annotations = row.get("annotations", [])
        if not annotations:
            continue
        image_data = convert_annotation_to_image_data(
            annotation=annotations[-1],
            bboxes_from_name="bbox",
            keypoints_from_name="kp",
            keypoints_labels=KEYPOINTS_LABELS,
            image_path=row["image_name"],
        )
        bboxes, labels, keypoints = [], [], []
        for bbox_data in image_data.bboxes_data:
            if bbox_data.label is None:
                continue
            label = str(bbox_data.label).lower()
            if label not in CLASSES_TO_KEEP:
                continue
            bboxes.append(list(bbox_data.coords))
            labels.append(label)
            keypoints.append([] if bbox_data.keypoints is None else np.array(bbox_data.keypoints).reshape(-1, 2).tolist())
        records.append({"image_name": row["image_name"], "bboxes": bboxes, "labels": labels, "keypoints": keypoints})
    return pd.DataFrame(records, columns=["image_name", "bboxes", "labels", "keypoints"])


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


def untracked_inference(
    *dfs: pd.DataFrame,
    ds: DataStore,
    best_model_table: str,
    trained_models_table: str,
    fallback_model_table: str,
    model_id_column: str,
    **kwargs,
) -> pd.DataFrame:
    s3_images_df = dfs[0]
    if s3_images_df.empty:
        return pd.DataFrame()
    best_model_id_df = ds.get_table(best_model_table).get_data()
    best_model_df = pd.DataFrame()
    if not best_model_id_df.empty:
        best_model_df = best_model_id_df.merge(ds.get_table(trained_models_table).get_data(), on=model_id_column)
    if best_model_df.empty:
        best_model_df = ds.get_table(fallback_model_table).get_data()
    return keypoints_inference(s3_images_df, best_model_df, **kwargs)


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


def publish_to_fiftyone(images_df: pd.DataFrame, predictions_df: pd.DataFrame, **kwargs) -> pd.DataFrame:
    return convert_df_with_bbox_to_df_with_image_data(
        df__with_bbox=pd.merge(predictions_df, images_df, on=kwargs["primary_keys"][0]),
        **kwargs,
    )

