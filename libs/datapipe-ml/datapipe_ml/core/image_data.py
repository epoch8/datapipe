import uuid
from typing import Any, List, Optional, cast

import numpy as np
import pandas as pd
from cv_pipeliner.core.data import BboxData, ImageData
from joblib import Parallel, delayed


def check_if_images_opens(image_paths: List[str], max_workers: int = 16) -> List[bool]:
    def thread_func(image_path: str):
        try:
            ImageData(image_path=image_path).open_image()
            return True
        except KeyboardInterrupt:
            raise
        except Exception as e:  # Некоторые файлы могут быть поломанными
            print(f"Exception catched for {image_path=}: {e=}")
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
    value = bbox_data.additional_info.get("prediction__keypoint_scores")
    if value is None:
        value = bbox_data.additional_info.get("prediction__keypoints_scores")
    return _as_list(value)


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
                            mask=row.get("mask"),
                            label=row.get("label"),
                            detection_score=row.get("prediction__detection_score"),
                            classification_score=row.get("prediction__classification_score"),
                            additional_info=(
                                {"prediction__keypoint_scores": row.get("prediction__keypoint_scores")}
                                if row.get("prediction__keypoint_scores") is not None
                                else {}
                            ),
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
    bboxes = row.get("bboxes", [])
    all_keypoints = row.get("keypoints", [[]] * len(bboxes))
    masks = row.get("masks", [[[]]] * len(bboxes))
    labels = row.get("labels", [None] * len(bboxes))
    if labels is None:
        labels = [None] * len(bboxes)
    prediction__detection_scores = row.get("prediction__detection_scores", [None] * len(bboxes))
    if prediction__detection_scores is None:
        prediction__detection_scores = [None] * len(bboxes)
    prediction__classification_scores = row.get("prediction__classification_scores", [None] * len(bboxes))
    if prediction__classification_scores is None:
        prediction__classification_scores = [None] * len(bboxes)
    prediction__keypoints_scores = row.get("prediction__keypoints_scores", [None] * len(bboxes))
    if prediction__keypoints_scores is None:
        prediction__keypoints_scores = [None] * len(bboxes)

    if len(labels) != len(bboxes):
        raise ValueError(f"Len {len(labels)=} not equal to {len(bboxes)=}")
    if len(prediction__detection_scores) != len(bboxes):
        raise ValueError(f"Len {len(prediction__detection_scores)=} not equal to {len(bboxes)=}")
    if len(prediction__classification_scores) != len(bboxes):
        raise ValueError(f"Len {len(prediction__classification_scores)=} not equal to {len(bboxes)=}")
    if len(prediction__keypoints_scores) != len(bboxes):
        raise ValueError(f"Len {len(prediction__keypoints_scores)=} not equal to {len(bboxes)=}")
    return [
        BboxData(
            xmin=xmin,
            ymin=ymin,
            xmax=xmax,
            ymax=ymax,
            label=label,
            keypoints=keypoints,
            mask=mask,
            detection_score=prediction__detection_score,
            classification_score=prediction__classification_score,
            additional_info=(
                {"prediction__keypoint_scores": prediction__keypoint_scores}
                if prediction__keypoint_scores is not None
                else {}
            ),
        )
        for (
            (xmin, ymin, xmax, ymax),
            keypoints,
            mask,
            label,
            prediction__detection_score,
            prediction__classification_score,
            prediction__keypoint_scores,
        ) in zip(
            bboxes,
            all_keypoints,
            masks,
            labels,
            prediction__detection_scores,
            prediction__classification_scores,
            prediction__keypoints_scores,
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
    n_records = [
        (
            [
                dict(
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
                    prediction__keypoint_scores=_get_prediction_keypoint_scores(bbox_data),
                )
                for bbox_data in cast(List[BboxData], row["image_data"].bboxes_data)
            ]
            if len(row["image_data"].bboxes_data) > 0
            else [
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
                    prediction__keypoint_scores=None,
                )
            ]
        )
        for _, row in df__with_image_data.iterrows()
    ]
    records = [record for records in n_records for record in records]
    return pd.DataFrame(
        records,
        columns=primary_keys
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
            "prediction__keypoint_scores",
        ],
    )


def _convert_df_with_image_data_to_df_with_bbox_json(
    df__with_image_data: pd.DataFrame,
    primary_keys: List[str],
    image__image_path__name: Optional[str] = None,
) -> pd.DataFrame:
    return pd.DataFrame(
        [
            dict(
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
            )
            for _, row in df__with_image_data.iterrows()
        ],
        columns=(
            primary_keys
            + ([image__image_path__name] if image__image_path__name is not None else [])
            + [
                "bboxes",
                "labels",
                "keypoints",
                "masks",
                "prediction__detection_scores",
                "prediction__classification_scores",
                "prediction__keypoints_scores",
            ]
        ),
    )


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
