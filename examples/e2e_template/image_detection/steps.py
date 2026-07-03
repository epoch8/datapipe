from __future__ import annotations

import os
from typing import Iterator

import fsspec
import pandas as pd
from cv_pipeliner.utils.label_studio import convert_annotation_to_image_data, convert_image_data_to_annotation
from datapipe_ml.core.image_data import (
    convert_df_with_bbox_to_df_with_image_data,
    convert_df_with_image_data_to_df_with_bbox,
)

from config import (
    DETECTION_MODEL_CONFIG,
    INPUT_IMAGES_DIR,
    LOCAL_IMAGES_DIR,
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


def list_detection_models() -> Iterator[pd.DataFrame]:
    yield pd.DataFrame([DETECTION_MODEL_CONFIG])


def resolve_best_detection_model(
    models_df: pd.DataFrame,
    best_model_df: pd.DataFrame,
    model_id_column: str,
    fallback_model_id: str,
) -> pd.DataFrame:
    if not best_model_df.empty:
        return best_model_df[[model_id_column]]
    return models_df[models_df[model_id_column] == fallback_model_id][[model_id_column]]


def get_images_without_ground_truth(
    images_df: pd.DataFrame,
    ground_truth_df: pd.DataFrame,
    primary_keys: list[str],
) -> pd.DataFrame:
    if ground_truth_df.empty:
        return images_df[primary_keys]
    annotated = ground_truth_df[primary_keys].drop_duplicates()
    merged = images_df.merge(annotated, on=primary_keys, how="left", indicator=True)
    return merged[merged["_merge"] == "left_only"][primary_keys]


def filter_bboxes_by_classes(
    df: pd.DataFrame,
    classes_to_keep: set[str],
    primary_keys: list[str],
    model_id_column: str,
) -> pd.DataFrame:
    if df.empty:
        return df

    row_keys = list(dict.fromkeys(primary_keys + [model_id_column]))
    df__image_data = convert_df_with_bbox_to_df_with_image_data(
        df__with_bbox=df,
        primary_keys=row_keys,
        bbox_id__name=None,
    )
    for image_data in df__image_data["image_data"]:
        image_data.bboxes_data = [
            bbox_data for bbox_data in image_data.bboxes_data if bbox_data.label in classes_to_keep
        ]
    return convert_df_with_image_data_to_df_with_bbox(
        df__with_image_data=df__image_data,
        primary_keys=row_keys,
        bbox_id__name=None,
    )[df.columns]


def bboxes_to_ls_prediction(
    df_bboxes: pd.DataFrame,
    df_image: pd.DataFrame,
    image__image_path__name: str,
    primary_keys: list[str],
    model_keys: list[str],
) -> pd.DataFrame:
    task_join_keys = [key for key in primary_keys if key not in set(model_keys)]
    output_keys = list(dict.fromkeys(primary_keys + model_keys))

    df_bboxes = df_bboxes.merge(
        df_image[task_join_keys + [image__image_path__name]],
        on=task_join_keys,
    )
    if df_bboxes.empty:
        return pd.DataFrame(columns=output_keys + ["prediction"])

    df__image_data = convert_df_with_bbox_to_df_with_image_data(
        df__with_bbox=df_bboxes,
        primary_keys=output_keys,
        bbox_id__name=None,
        image__image_path__name=image__image_path__name,
    )
    df__image_data["prediction"] = df__image_data["image_data"].apply(
        lambda image_data: {
            "result": convert_image_data_to_annotation(
                image_data=image_data,
                to_name="image",
                bboxes_from_name="label",
            )
        }
    )
    return df__image_data[output_keys + ["prediction"]]


def parse_annotations_from_label_studio(df: pd.DataFrame) -> pd.DataFrame:
    records = []
    for _, row in df.iterrows():
        annotations = row.get("annotations", [])
        if not annotations:
            continue
        image_data = convert_annotation_to_image_data(
            annotation=annotations[-1],
            bboxes_from_name="label",
            image_path=row["image_name"],
        )
        records.append(
            {
                "image_name": row["image_name"],
                "bboxes": [bbox_data.coords for bbox_data in image_data.bboxes_data],
                "labels": [str(bbox_data.label) for bbox_data in image_data.bboxes_data],
            }
        )
    return pd.DataFrame(records, columns=["image_name", "bboxes", "labels"])


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


def publish_to_fiftyone(images_df: pd.DataFrame, predictions_df: pd.DataFrame, **kwargs) -> pd.DataFrame:
    return convert_df_with_bbox_to_df_with_image_data(
        df__with_bbox=pd.merge(predictions_df, images_df, on=kwargs["primary_keys"][0]),
        **kwargs,
    )
