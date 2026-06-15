from __future__ import annotations

import json
from typing import Dict, List, Optional, Tuple, cast

import cityhash
import fsspec
import pandas as pd
from cv_pipeliner.core.data import ImageData
from cv_pipeliner.data_converters.yolo import YOLODataConverter, YOLOMasksDataConverter
from cv_pipeliner.utils.images_datas import thumbnail_image_data
from datapipe.store.filedir import ItemStoreFileAdapter
from datapipe.types import DataDF, HashDF, IndexDF
from natsort import natsorted
from pathy import Pathy

from datapipe_ml.core.image_data import convert_df_with_bbox_to_df_with_image_data
from datapipe_ml.utils.fsspec_storage import fsspec_storage_options


class CustomYOLOLabelsFile(ItemStoreFileAdapter):
    mode = "b"

    def __init__(self, img_format: str):
        self.img_format = img_format

    def hash_rows(self, df: DataDF, keys: List[str]) -> HashDF:
        hash_df = df[keys]
        hash_df["hash"] = df.apply(lambda x: str(list(x)), axis=1).apply(
            lambda x: int.from_bytes(cityhash.CityHash32(x).to_bytes(4, "little"), "little", signed=True)
        )
        return cast(HashDF, hash_df)

    def load(self, f: fsspec.core.OpenFile) -> Dict[str, Tuple[int, int]]:
        filepath = Pathy.fluid(f.path)
        assert filepath.parent.name == "labels"  # labels
        assert f.fs.exists(str(filepath.parent.parent.parent.parent / "class_names.json"))
        with f.fs.open(str(filepath.parent.parent.parent.parent / "class_names.json"), "r") as src:
            class_names = json.load(src)
        image_path = filepath.parent.parent / "images" / f"{filepath.stem}.{self.img_format}"
        yolo_converter = YOLODataConverter(class_names)
        if f.fs.protocol in ["file"] or f.fs.protocol is None:
            prefix = ""
        else:
            protocol = f.fs.protocol
            if isinstance(protocol, tuple):
                protocol = protocol[0]
            prefix = f"{protocol}://"
        image_data = yolo_converter.get_image_data_from_annot(image_path=f"{prefix}{image_path}", annot=f)
        return {"image_size": image_data, "class_names": class_names}

    def dump(self, obj: Dict[str, Tuple[int, int]], f: fsspec.core.OpenFile) -> None:
        image_data: ImageData = obj["image_data"]
        filepath = Pathy.fluid(f.path)
        assert filepath.parent.name == "labels"
        assert f.fs.exists(str(filepath.parent.parent.parent.parent / "class_names.json"))
        with f.fs.open(str(filepath.parent.parent.parent.parent / "class_names.json"), "r") as src:
            class_names = json.load(src)
        yolo_converter = YOLODataConverter(class_names)
        coco_data = yolo_converter.get_annot_from_image_data(image_data)
        f.write("\n".join(coco_data).encode())


class CustomYOLOV8SegmentatorLabelsFile(ItemStoreFileAdapter):
    mode = "b"

    def __init__(self, img_format: str):
        self.img_format = img_format

    def hash_rows(self, df: DataDF, keys: List[str]) -> HashDF:
        hash_df = df[keys]
        hash_df["hash"] = df.apply(lambda x: str(list(x)), axis=1).apply(
            lambda x: int.from_bytes(cityhash.CityHash32(x).to_bytes(4, "little"), "little", signed=True)
        )
        return cast(HashDF, hash_df)

    def load(self, f: fsspec.core.OpenFile) -> Dict[str, Tuple[int, int]]:
        filepath = Pathy.fluid(f.path)
        assert filepath.parent.name == "labels"  # labels
        assert f.fs.exists(str(filepath.parent.parent.parent.parent / "class_names.json"))
        with f.fs.open(str(filepath.parent.parent.parent.parent / "class_names.json"), "r") as src:
            class_names = json.load(src)
        image_path = filepath.parent.parent / "images" / f"{filepath.stem}.{self.img_format}"
        yolo_converter = YOLOMasksDataConverter(class_names)
        if f.fs.protocol in ["file"] or f.fs.protocol is None:
            prefix = ""
        else:
            protocol = f.fs.protocol
            if isinstance(protocol, tuple):
                protocol = protocol[0]
            prefix = f"{protocol}://"
        image_data = yolo_converter.get_image_data_from_annot(image_path=f"{prefix}{image_path}", annot=f)
        return {"image_size": image_data, "class_names": class_names}

    def dump(self, obj: Dict[str, Tuple[int, int]], f: fsspec.core.OpenFile) -> None:
        image_data: ImageData = obj["image_data"]
        filepath = Pathy.fluid(f.path)
        assert filepath.parent.name == "labels"
        assert f.fs.exists(str(filepath.parent.parent.parent.parent / "class_names.json"))
        with f.fs.open(str(filepath.parent.parent.parent.parent / "class_names.json"), "r") as src:
            class_names = json.load(src)
        yolo_converter = YOLOMasksDataConverter(class_names)
        coco_segmentation_data = yolo_converter.get_annot_from_image_data(image_data)
        f.write("\n".join(coco_segmentation_data).encode())


class CustomYOLOV8PoseLabelsFile(ItemStoreFileAdapter):
    mode = "b"

    def __init__(self, img_format: str):
        self.img_format = img_format

    def hash_rows(self, df: DataDF, keys: List[str]) -> HashDF:
        hash_df = df[keys]
        hash_df["hash"] = df.apply(lambda x: str(list(x)), axis=1).apply(
            lambda x: int.from_bytes(cityhash.CityHash32(x).to_bytes(4, "little"), "little", signed=True)
        )
        return cast(HashDF, hash_df)

    def load(self, f: fsspec.core.OpenFile) -> Dict[str, Tuple[int, int]]:
        filepath = Pathy.fluid(f.path)
        assert filepath.parent.name == "labels"
        assert f.fs.exists(str(filepath.parent.parent.parent.parent / "class_names.json"))
        with f.fs.open(str(filepath.parent.parent.parent.parent / "class_names.json"), "r") as src:
            class_names = json.load(src)
        image_path = filepath.parent.parent / "images" / f"{filepath.stem}.{self.img_format}"
        yolo_converter = YOLOPoseDataConverter(class_names)
        if f.fs.protocol in ["file"] or f.fs.protocol is None:
            prefix = ""
        else:
            protocol = f.fs.protocol
            if isinstance(protocol, tuple):
                protocol = protocol[0]
            prefix = f"{protocol}://"
        image_data = yolo_converter.get_image_data_from_annot(image_path=f"{prefix}{image_path}", annot=f)
        return {"image_size": image_data, "class_names": class_names}

    def dump(self, obj: Dict[str, Tuple[int, int]], f: fsspec.core.OpenFile) -> None:
        image_data: ImageData = obj["image_data"]
        filepath = Pathy.fluid(f.path)
        assert filepath.parent.name == "labels"
        assert f.fs.exists(str(filepath.parent.parent.parent.parent / "class_names.json"))
        with f.fs.open(str(filepath.parent.parent.parent.parent / "class_names.json"), "r") as src:
            class_names = json.load(src)
        yolo_converter = YOLOPoseDataConverter(class_names)
        pose_data = yolo_converter.get_annot_from_image_data(image_data)
        f.write("\n".join(pose_data).encode())


class YOLOPoseDataConverter(YOLODataConverter):
    def get_annot_from_image_data(self, image_data: ImageData) -> List[str]:
        image_data = self.filter_image_data(image_data)
        width, height = image_data.get_image_size()
        txt_results = []
        for bbox_data in image_data.bboxes_data:
            w = bbox_data.xmax - bbox_data.xmin
            h = bbox_data.ymax - bbox_data.ymin
            xcenter = bbox_data.xmin + w / 2
            ycenter = bbox_data.ymin + h / 2
            parts = [
                str(self.class_name_to_idx[bbox_data.label]),
                str(round(xcenter / width, 6)),
                str(round(ycenter / height, 6)),
                str(round(w / width, 6)),
                str(round(h / height, 6)),
            ]
            keypoints = bbox_data.keypoints if bbox_data.keypoints is not None else []
            visibility = None
            if isinstance(bbox_data.additional_info, dict):
                visibility = bbox_data.additional_info.get("keypoints_visibility")
            if visibility is None:
                visibility = [2] * len(keypoints)
            for (x, y), visible in zip(keypoints, visibility):
                parts.extend([str(round(x / width, 6)), str(round(y / height, 6)), str(int(visible))])
            txt_results.append(" ".join(parts))
        return txt_results


def get_class_names_from_det_frozen_dataset_gt(
    df__detection_frozen_dataset__has__image_gt: pd.DataFrame,
    bbox_id__name: Optional[str],
    filedir: Pathy,
    detection_model_other_primary_keys: List[str],
    detection_frozen_dataset_id__name: str,
    idx: IndexDF,
    **kwargs,
):
    if bbox_id__name is None:
        df__detection_frozen_dataset__has__image_gt = df__detection_frozen_dataset__has__image_gt.explode("labels")
        df__detection_frozen_dataset__has__image_gt.rename(columns={"labels": "label"}, inplace=True)
    df__detection_frozen_dataset__has__image_gt.dropna(subset=["label"], inplace=True)
    if len(df__detection_frozen_dataset__has__image_gt) == 0:
        return pd.DataFrame(columns=[detection_frozen_dataset_id__name, "class_names"])
    detection_frozen_dataset_ids = set(
        list(df__detection_frozen_dataset__has__image_gt[detection_frozen_dataset_id__name])
    )
    assert len(detection_frozen_dataset_ids) == 1
    detection_frozen_dataset_id = list(detection_frozen_dataset_ids)[0]
    class_names = natsorted(list(set(df__detection_frozen_dataset__has__image_gt["label"])))
    filepath = (
        filedir
        / ("/".join([idx.iloc[0][primary_key] for primary_key in detection_model_other_primary_keys]))
        / detection_frozen_dataset_id
        / "class_names.json"
    )
    filepath_str = str(filepath)
    with fsspec.open(filepath_str, "w", **fsspec_storage_options(filepath_str)) as out:
        json.dump(class_names, out, ensure_ascii=False)
    return pd.DataFrame(
        [
            {
                **{primary_key: idx.iloc[0][primary_key] for primary_key in detection_model_other_primary_keys},
                detection_frozen_dataset_id__name: detection_frozen_dataset_id,
                "class_names": class_names,
            }
        ]
    )


def get_class_names_from_kps_frozen_dataset_gt(
    df__keypoints_frozen_dataset__has__image_gt: pd.DataFrame,
    bbox_id__name: Optional[str],
    filedir: Pathy,
    detection_model_other_primary_keys: List[str],
    detection_frozen_dataset_id__name: str,
    idx: IndexDF,
    **kwargs,
):
    def _is_null_like(value):
        return value is None or (isinstance(value, float) and pd.isna(value))

    if bbox_id__name is None:
        df__keypoints_frozen_dataset__has__image_gt = df__keypoints_frozen_dataset__has__image_gt.explode(
            ["labels", "keypoints"]
        )
        df__keypoints_frozen_dataset__has__image_gt.rename(columns={"labels": "label"}, inplace=True)
    df__keypoints_frozen_dataset__has__image_gt.dropna(subset=["label"], inplace=True)
    if len(df__keypoints_frozen_dataset__has__image_gt) == 0:
        return pd.DataFrame(columns=[detection_frozen_dataset_id__name, "class_names", "kpt_shape", "flip_idx"])
    frozen_dataset_ids = set(list(df__keypoints_frozen_dataset__has__image_gt[detection_frozen_dataset_id__name]))
    assert len(frozen_dataset_ids) == 1
    frozen_dataset_id = list(frozen_dataset_ids)[0]
    class_names = natsorted(list(set(df__keypoints_frozen_dataset__has__image_gt["label"])))
    kpt_shape = None
    for keypoints_value in df__keypoints_frozen_dataset__has__image_gt["keypoints"]:
        if keypoints_value is None:
            continue
        if isinstance(keypoints_value, list) and len(keypoints_value) > 0:
            first_item = keypoints_value[0]
            if isinstance(first_item, list) and first_item and isinstance(first_item[0], list):
                kpt_shape = [len(first_item), 3]
            else:
                kpt_shape = [len(keypoints_value), 3]
            break
    if kpt_shape is None:
        raise ValueError("No keypoints found in the keypoints frozen dataset ground truth.")

    flip_idx = None
    if "flip_idx" in df__keypoints_frozen_dataset__has__image_gt.columns:
        flip_idx_values = [
            value
            for value in df__keypoints_frozen_dataset__has__image_gt["flip_idx"].tolist()
            if not _is_null_like(value)
        ]
        if len(flip_idx_values) > 0:
            flip_idx = flip_idx_values[0]
            unique_flip_idx = {json.dumps(value, sort_keys=True, ensure_ascii=False) for value in flip_idx_values}
            if len(unique_flip_idx) > 1:
                raise ValueError("Different flip_idx values found in single keypoints frozen dataset.")

    filepath = (
        filedir
        / ("/".join([idx.iloc[0][primary_key] for primary_key in detection_model_other_primary_keys]))
        / frozen_dataset_id
        / "class_names.json"
    )
    filepath_str = str(filepath)
    with fsspec.open(filepath_str, "w", **fsspec_storage_options(filepath_str)) as out:
        json.dump(class_names, out, ensure_ascii=False)
    return pd.DataFrame(
        [
            {
                **{primary_key: idx.iloc[0][primary_key] for primary_key in detection_model_other_primary_keys},
                detection_frozen_dataset_id__name: frozen_dataset_id,
                "class_names": class_names,
                "kpt_shape": kpt_shape,
                "flip_idx": flip_idx,
            }
        ]
    )


get_class_names_from_keypoints_frozen_dataset_gt = get_class_names_from_kps_frozen_dataset_gt


def resize_and_prepare_yolo_images(
    df__detection_frozen_dataset__has__image_gt: pd.DataFrame,
    df__detection_size_for_resize: pd.DataFrame,
    df__detection_frozen_dataset__class_names: pd.DataFrame,
    primary_keys: List[str],
    bbox_id__name: Optional[str],
    image__image_path__name: str,
    detection_model_other_primary_keys: List[str],
    detection_frozen_dataset_id__name: str,
    **kwargs,
):
    columns = (
        [detection_frozen_dataset_id__name]
        + list(set(primary_keys + detection_model_other_primary_keys))
        + ["subset_id", "width", "height"]
    )
    df__image_data = convert_df_with_bbox_to_df_with_image_data(
        df__with_bbox=df__detection_frozen_dataset__has__image_gt,
        primary_keys=primary_keys + [detection_frozen_dataset_id__name, "subset_id"],
        bbox_id__name=bbox_id__name,
        image__image_path__name=image__image_path__name,
    )
    if len(df__image_data) == 0 or len(df__detection_size_for_resize) == 0:
        return pd.DataFrame(columns=columns + ["image"]), pd.DataFrame(columns=columns + ["image_data"])
    df__image_data = pd.merge(df__image_data, df__detection_size_for_resize, how="cross")
    df__image_data["image_data"] = df__image_data.apply(
        lambda row: (
            thumbnail_image_data(row["image_data"], (row["width"], row["height"]))
            if row["width"] != -1 and row["height"] != -1
            else row["image_data"]
        ),
        axis=1,
    )
    df__image_data.drop_duplicates(subset=columns, inplace=True)
    df__image_data["image"] = df__image_data["image_data"].apply(lambda image_data: image_data.open_image())
    return df__image_data[columns + ["image"]], df__image_data[columns + ["image_data"]]


def get_size_for_resize(
    df__detection_train_config_id: pd.DataFrame,
    resize_images: bool,
    train_config_id_col: str,
    train_config_params_col: str,
):
    if resize_images:
        df__detection_train_config_id["height"] = df__detection_train_config_id[train_config_params_col].apply(
            lambda params: params["imgsz"]
        )
    else:
        df__detection_train_config_id["height"] = -1
    df__detection_train_config_id["width"] = df__detection_train_config_id["height"]
    return df__detection_train_config_id[[train_config_id_col, "width", "height"]]
