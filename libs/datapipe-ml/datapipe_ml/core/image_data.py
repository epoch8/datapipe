import logging
import uuid
from typing import Any, Dict, List, Optional, cast

import numpy as np
import pandas as pd
from cv_pipeliner.core.data import BboxData, ImageData
from joblib import Parallel, delayed
from PIL import UnidentifiedImageError

logger = logging.getLogger(__name__)


def check_if_images_opens(image_paths: List[str], max_workers: int = 16) -> List[bool]:
    def thread_func(image_path: str):
        try:
            ImageData(image_path=image_path).open_image()
            return True
        except KeyboardInterrupt:
            raise
        except (OSError, UnidentifiedImageError) as e:
            logger.warning("Failed to open image %s: %s", image_path, e)
            return False

    return Parallel(n_jobs=max_workers, prefer="threads")(
        delayed(thread_func)(image_path) for image_path in image_paths
    )


def get_bbox_id(primary_keys: List[str], primary_keys__values: List[Any], bbox_data: BboxData):
    idxs = np.argsort(primary_keys)
    primary_keys__values = [primary_keys__values[idx] for idx in idxs]
    image_id = "__".join([str(value) for value in primary_keys__values])
    bbox_coords = "_".join([str(coord) for coord in bbox_data.coords])
    bbox_id = str(uuid.uuid5(uuid.NAMESPACE_X500, f"{image_id}_{bbox_coords}"))
    return bbox_id


def _as_list(value: Any):
    if value is None:
        return None
    if isinstance(value, np.ndarray):
        return value.tolist()
    return value


def _mask_as_list(mask: Any):
    if mask is None:
        return []
    return [[_as_list(point) for point in polygon] for polygon in mask]


def _get_prediction_keypoint_scores(bbox_data: BboxData):
    return _as_list(bbox_data.keypoints_scores)


def _get_keypoints_visibility(bbox_data: BboxData):
    value = bbox_data.keypoints_visibility
    if value is None:
        return None
    return [int(v) for v in value]


def _get_additional_info(obj: Any) -> Dict[str, Any]:
    value = getattr(obj, "additional_info", None)
    if value is None:
        return {}
    return dict(value)


def _aligned_per_bbox_field(row: pd.Series, field: str, *, length: int, default: Any) -> list[Any]:
    values = row.get(field, [default] * length)
    if values is None:
        return [default] * length
    if len(values) != length:
        raise ValueError(f"Len {field}={len(values)} not equal to len bboxes={length}")
    return list(values)


def _convert_df_with_bbox_rows_to_df_with_image_data(
    df__with_bbox_rows: pd.DataFrame,
    primary_keys: List[str],
    bbox_id__name: str,
    image__image_path__name: Optional[str] = None,
) -> pd.DataFrame:
    return pd.DataFrame(
        [
            dict(
                **{primary_key: primary_keys__values[idx] for idx, primary_key in enumerate(primary_keys)},
                image_data=ImageData(
                    image_path=(
                        df__grouped.iloc[0][image__image_path__name] if image__image_path__name is not None else None
                    ),
                    bboxes_data=[
                        BboxData(
                            xmin=row.get("x_min"),
                            ymin=row.get("y_min"),
                            xmax=row.get("x_max"),
                            ymax=row.get("y_max"),
                            keypoints=row.get("keypoints"),
                            keypoints_visibility=row.get("keypoints_visibility"),
                            keypoints_scores=(
                                row.get("prediction__keypoints_scores")
                                if row.get("prediction__keypoints_scores") is not None
                                else row.get("prediction__keypoint_scores")
                            ),
                            mask=row.get("mask"),
                            label=row.get("label"),
                            detection_score=row.get("prediction__detection_score"),
                            classification_score=row.get("prediction__classification_score"),
                            additional_info=dict(row.get("additional_info") or {}),
                        )
                        for _, row in df__grouped.iterrows()
                        if row[bbox_id__name] != "None"
                    ],
                ),
            )
            for primary_keys__values, df__grouped in df__with_bbox_rows.groupby(primary_keys)
        ],
        columns=primary_keys + ["image_data"],
    )


def get_bboxes_data_from_json(row: pd.Series):
    bboxes = row.get("bboxes", []) or []
    all_keypoints = _aligned_per_bbox_field(row, "keypoints", length=len(bboxes), default=[])
    masks = _aligned_per_bbox_field(row, "masks", length=len(bboxes), default=[[]])
    labels = _aligned_per_bbox_field(row, "labels", length=len(bboxes), default=None)
    prediction__detection_scores = _aligned_per_bbox_field(
        row, "prediction__detection_scores", length=len(bboxes), default=None
    )
    prediction__classification_scores = _aligned_per_bbox_field(
        row, "prediction__classification_scores", length=len(bboxes), default=None
    )
    prediction__keypoints_scores = _aligned_per_bbox_field(
        row, "prediction__keypoints_scores", length=len(bboxes), default=None
    )
    keypoints_visibility = _aligned_per_bbox_field(row, "keypoints_visibility", length=len(bboxes), default=None)
    bboxes_additional_infos = _aligned_per_bbox_field(
        row, "bboxes_additional_infos", length=len(bboxes), default={}
    )

    return [
        BboxData(
            xmin=xmin,
            ymin=ymin,
            xmax=xmax,
            ymax=ymax,
            label=label,
            keypoints=keypoints,
            keypoints_visibility=keypoints_visibility_item,
            keypoints_scores=prediction__keypoints_score,
            mask=mask,
            detection_score=prediction__detection_score,
            classification_score=prediction__classification_score,
            additional_info=dict(additional_info_item or {}),
        )
        for (
            (xmin, ymin, xmax, ymax),
            keypoints,
            mask,
            label,
            prediction__detection_score,
            prediction__classification_score,
            prediction__keypoints_score,
            keypoints_visibility_item,
            additional_info_item,
        ) in zip(
            bboxes,
            all_keypoints,
            masks,
            labels,
            prediction__detection_scores,
            prediction__classification_scores,
            prediction__keypoints_scores,
            keypoints_visibility,
            bboxes_additional_infos,
        )
    ]


def _convert_df_with_bbox_json_to_df_with_image_data(
    df__with_bbox_json: pd.DataFrame,
    primary_keys: List[str],
    image__image_path__name: Optional[str] = None,
) -> pd.DataFrame:
    return pd.DataFrame(
        [
            dict(
                **{primary_key: row[primary_key] for primary_key in primary_keys},
                image_data=ImageData(
                    image_path=(row[image__image_path__name] if image__image_path__name is not None else None),
                    additional_info=dict(row.get("additional_info") or {}),
                    bboxes_data=get_bboxes_data_from_json(row),
                ),
            )
            for _, row in df__with_bbox_json.iterrows()
        ],
        columns=primary_keys + ["image_data"],
    )


def convert_df_with_bbox_to_df_with_image_data(
    df__with_bbox: pd.DataFrame,
    primary_keys: List[str],
    bbox_id__name: Optional[str] = None,
    image__image_path__name: Optional[str] = None,
) -> pd.DataFrame:
    if bbox_id__name is not None:
        return _convert_df_with_bbox_rows_to_df_with_image_data(
            df__with_bbox_rows=df__with_bbox,
            primary_keys=primary_keys,
            bbox_id__name=bbox_id__name,
            image__image_path__name=image__image_path__name,
        )
    else:
        return _convert_df_with_bbox_json_to_df_with_image_data(
            df__with_bbox_json=df__with_bbox,
            primary_keys=primary_keys,
            image__image_path__name=image__image_path__name,
        )


def _convert_df_with_image_data_to_df_with_bbox_rows(
    df__with_image_data: pd.DataFrame,
    primary_keys: List[str],
    bbox_id__name: str,
    image__image_path__name: Optional[str] = None,
) -> pd.DataFrame:
    n_records = []
    for _, row in df__with_image_data.iterrows():
        if len(row["image_data"].bboxes_data) > 0:
            for bbox_data in cast(List[BboxData], row["image_data"].bboxes_data):
                record = dict(
                    **{primary_key: row[primary_key] for primary_key in primary_keys},
                    **{
                        bbox_id__name: get_bbox_id(
                            primary_keys,
                            primary_keys__values=[row[primary_key] for primary_key in primary_keys],
                            bbox_data=bbox_data,
                        )
                    },
                    **(
                        {image__image_path__name: str(row["image_data"].image_path)}
                        if image__image_path__name is not None
                        else {}
                    ),
                    x_min=bbox_data.xmin,
                    y_min=bbox_data.ymin,
                    x_max=bbox_data.xmax,
                    y_max=bbox_data.ymax,
                    keypoints=bbox_data.keypoints,
                    mask=bbox_data.mask,
                    label=bbox_data.label,
                    prediction__detection_score=bbox_data.detection_score,
                    prediction__classification_score=bbox_data.classification_score,
                    prediction__keypoints_scores=_get_prediction_keypoint_scores(bbox_data),
                    keypoints_visibility=_get_keypoints_visibility(bbox_data),
                )
                additional_info = _get_additional_info(bbox_data)
                if additional_info:
                    record["additional_info"] = additional_info
                n_records.append(record)
        else:
            n_records.append(
                dict(
                    **{primary_key: row[primary_key] for primary_key in primary_keys},
                    **{bbox_id__name: "None"},
                    **(
                        {image__image_path__name: str(row["image_data"].image_path)}
                        if image__image_path__name is not None
                        else {}
                    ),
                    x_min=None,
                    y_min=None,
                    x_max=None,
                    y_max=None,
                    keypoints=None,
                    mask=None,
                    label=None,
                    prediction__detection_score=None,
                    prediction__classification_score=None,
                    prediction__keypoints_scores=None,
                    keypoints_visibility=None,
                )
            )

    columns = (
        primary_keys
        + [bbox_id__name]
        + ([image__image_path__name] if image__image_path__name is not None else [])
        + [
            "x_min",
            "y_min",
            "x_max",
            "y_max",
            "label",
            "keypoints",
            "mask",
            "prediction__detection_score",
            "prediction__classification_score",
            "prediction__keypoints_scores",
            "keypoints_visibility",
        ]
    )
    if any(record.get("additional_info") for record in n_records):
        columns.append("additional_info")
    return pd.DataFrame(n_records, columns=columns)


def _convert_df_with_image_data_to_df_with_bbox_json(
    df__with_image_data: pd.DataFrame,
    primary_keys: List[str],
    image__image_path__name: Optional[str] = None,
) -> pd.DataFrame:
    records = []
    for _, row in df__with_image_data.iterrows():
        record = dict(
            **{primary_key: row[primary_key] for primary_key in primary_keys},
            **(
                {image__image_path__name: str(row["image_data"].image_path)}
                if image__image_path__name is not None
                else {}
            ),
            bboxes=[bbox_data.coords for bbox_data in row["image_data"].bboxes_data],
            keypoints=[_as_list(bbox_data.keypoints) for bbox_data in row["image_data"].bboxes_data],
            masks=[_mask_as_list(bbox_data.mask) for bbox_data in row["image_data"].bboxes_data],
            labels=[bbox_data.label for bbox_data in row["image_data"].bboxes_data],
            prediction__detection_scores=[bbox_data.detection_score for bbox_data in row["image_data"].bboxes_data],
            prediction__classification_scores=[
                bbox_data.classification_score for bbox_data in row["image_data"].bboxes_data
            ],
            prediction__keypoints_scores=[
                _get_prediction_keypoint_scores(bbox_data) for bbox_data in row["image_data"].bboxes_data
            ],
            keypoints_visibility=[
                _get_keypoints_visibility(bbox_data) for bbox_data in row["image_data"].bboxes_data
            ],
        )
        bboxes_additional_infos = [_get_additional_info(bbox_data) for bbox_data in row["image_data"].bboxes_data]
        additional_info = _get_additional_info(row["image_data"])
        if any(bboxes_additional_infos):
            record["bboxes_additional_infos"] = bboxes_additional_infos
        if additional_info:
            record["additional_info"] = additional_info
        records.append(record)

    columns = primary_keys + ([image__image_path__name] if image__image_path__name is not None else []) + [
        "bboxes",
        "labels",
        "keypoints",
        "masks",
        "prediction__detection_scores",
        "prediction__classification_scores",
        "prediction__keypoints_scores",
        "keypoints_visibility",
    ]
    if any("additional_info" in record for record in records):
        columns.append("additional_info")
    if any("bboxes_additional_infos" in record for record in records):
        columns.append("bboxes_additional_infos")
    return pd.DataFrame(records, columns=columns)


def convert_df_with_image_data_to_df_with_bbox(
    df__with_image_data: pd.DataFrame,
    primary_keys: List[str],
    bbox_id__name: Optional[str] = None,
    image__image_path__name: Optional[str] = None,
) -> pd.DataFrame:
    if bbox_id__name is not None:
        return _convert_df_with_image_data_to_df_with_bbox_rows(
            df__with_image_data=df__with_image_data,
            primary_keys=primary_keys,
            bbox_id__name=bbox_id__name,
            image__image_path__name=image__image_path__name,
        )
    else:
        return _convert_df_with_image_data_to_df_with_bbox_json(
            df__with_image_data=df__with_image_data,
            primary_keys=primary_keys,
            image__image_path__name=image__image_path__name,
        )
