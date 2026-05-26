from __future__ import annotations

import os
from pathlib import Path
from typing import Iterator

import fsspec
import pandas as pd
from cv_pipeliner import BboxData, ImageData
from cv_pipeliner.utils.label_studio import convert_annotation_to_image_data, convert_image_data_to_annotation
from datapipe.datatable import DataStore
from datapipe_ml.core.image_data import convert_df_with_bbox_to_df_with_image_data
from datapipe_ml.tasks.detection.inference import detection_inference

from examples.e2e_template.common import ServiceSettings
from examples.e2e_template.image_detection.config import CLASSES_TO_KEEP, DETECTION_MODEL_CONFIG, local_images_dir


def list_s3_images(settings: ServiceSettings) -> Iterator[pd.DataFrame]:
    import boto3

    s3 = boto3.client(
        "s3",
        aws_access_key_id=settings.aws_key,
        aws_secret_access_key=settings.aws_secret,
        region_name=settings.aws_region,
        endpoint_url=settings.s3_endpoint_url,
    )
    paginator = s3.get_paginator("list_objects_v2")
    keys, urls = [], []
    for page in paginator.paginate(Bucket=settings.s3_bucket, Prefix=settings.s3_prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.lower().endswith((".jpg", ".jpeg", ".png", ".webp")):
                keys.append(key.replace("/", "___"))
                urls.append(f"s3://{settings.s3_bucket}/{key}")

    yield pd.DataFrame({"image_name": keys, "image_url": urls})


def list_detection_models() -> Iterator[pd.DataFrame]:
    yield pd.DataFrame([DETECTION_MODEL_CONFIG])


def bboxes_to_ls_prediction(
    df_bboxes: pd.DataFrame,
    df_image: pd.DataFrame,
    image__image_path__name: str,
) -> pd.DataFrame:
    image_name_to_path = dict(zip(df_image["image_name"], df_image[image__image_path__name]))
    records = []
    for model_id, model_df in df_bboxes.groupby("detection_model_id"):
        for image_name, group in model_df.groupby("image_name"):
            image_data = ImageData(
                image_path=image_name_to_path[image_name],
                bboxes_data=[
                    BboxData(
                        xmin=row["x_min"],
                        ymin=row["y_min"],
                        xmax=row["x_max"],
                        ymax=row["y_max"],
                        label=str(row["label"]).capitalize(),
                    )
                    for _, row in group.iterrows()
                    if row["bbox_id"] != "None" and row["label"] in CLASSES_TO_KEEP
                ],
            )
            records.append(
                {
                    "image_name": image_name,
                    "prediction": {
                        "result": convert_image_data_to_annotation(
                            image_data=image_data,
                            to_name="image",
                            bboxes_from_name="label",
                        )
                    },
                    "detection_model_id": model_id,
                }
            )
    return pd.DataFrame(records, columns=["image_name", "prediction", "detection_model_id"])


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
                "labels": [str(bbox_data.label).lower() for bbox_data in image_data.bboxes_data],
            }
        )
    return pd.DataFrame(records, columns=["image_name", "bboxes", "labels"])


def split_df_train_val(df: pd.DataFrame, val_perc: float = 0.25) -> pd.DataFrame:
    train_size = int(len(df) * (1 - val_perc))
    df_subset = pd.DataFrame({"image_name": df["image_name"]})
    df_subset["subset_id"] = "val"
    df_subset.loc[:train_size, "subset_id"] = "train"
    return df_subset


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

    return detection_inference(s3_images_df, best_model_df, **kwargs)


def download_images(
    s3_images_df: pd.DataFrame,
    settings: ServiceSettings,
    image__image_path__name: str,
    image__local_image_path__name: str,
) -> pd.DataFrame:
    output_dir = local_images_dir(settings)
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

