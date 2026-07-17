from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

import numpy as np
import pandas as pd
from PIL import Image


@dataclass(frozen=True)
class SmokeDataset:
    image: pd.DataFrame
    image_ground_truth: pd.DataFrame
    subset_has_image: pd.DataFrame
    image_ground_truth_for_classification: pd.DataFrame
    image_ground_truth_for_keypoints: pd.DataFrame


def get_most_common_label(labels: list[str]) -> str:
    label_counts = Counter(labels)
    max_count = max(label_counts.values())
    return min(label for label, count in label_counts.items() if count == max_count)


def define_ground_truth_for_classification(df__image__ground_truth: pd.DataFrame) -> pd.DataFrame:
    df = df__image__ground_truth.copy()
    df["label"] = df["labels"].apply(get_most_common_label)
    return df[["image_id", "label"]]


def define_ground_truth_for_keypoints(df__image__ground_truth: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for _, row in df__image__ground_truth.iterrows():
        keypoints = []
        keypoints_visibility = []
        for x_min, y_min, x_max, y_max in row["bboxes"]:
            keypoints.append(
                [
                    [x_min, y_min],
                    [x_max, y_min],
                    [x_max, y_max],
                    [x_min, y_max],
                ]
            )
            keypoints_visibility.append([2, 2, 2, 2])
        rows.append(
            {
                "image_id": row["image_id"],
                "bboxes": row["bboxes"],
                "labels": row["labels"],
                "keypoints": keypoints,
                "keypoints_visibility": keypoints_visibility,
                "flip_idx": [1, 0, 3, 2],
            }
        )
    return pd.DataFrame(rows)


def make_smoke_dataset(images_dir: Path, *, size: int = 6) -> SmokeDataset:
    images_dir.mkdir(parents=True, exist_ok=True)
    image_rows = []
    gt_rows = []
    subset_rows = []
    labels_by_image = [
        ["cat", "dog"],
        ["cat"],
        ["dog"],
        ["cat", "cat"],
        ["dog", "cat"],
        ["cat"],
    ]

    for idx in range(size):
        image_id = f"image_{idx:03d}"
        image_path = images_dir / f"{image_id}.jpg"
        image = np.full((32, 32, 3), fill_value=20 + idx * 20, dtype=np.uint8)
        Image.fromarray(image).convert("RGB").save(image_path)

        labels = labels_by_image[idx % len(labels_by_image)]
        bboxes = []
        masks = []
        for bbox_idx, _label in enumerate(labels):
            offset = 2 + bbox_idx * 8
            bbox = [offset, offset, offset + 8, offset + 8]
            bboxes.append(bbox)
            masks.append([[bbox[0], bbox[1]], [bbox[2], bbox[1]], [bbox[2], bbox[3]], [bbox[0], bbox[3]]])

        image_rows.append({"image_id": image_id, "image__image_path": str(image_path)})
        gt_rows.append({"image_id": image_id, "bboxes": bboxes, "labels": labels, "masks": masks})
        subset_rows.append({"image_id": image_id, "subset_id": "val" if idx % 3 == 0 else "train"})

    image_ground_truth = pd.DataFrame(gt_rows)
    return SmokeDataset(
        image=pd.DataFrame(image_rows),
        image_ground_truth=image_ground_truth,
        subset_has_image=pd.DataFrame(subset_rows),
        image_ground_truth_for_classification=define_ground_truth_for_classification(image_ground_truth),
        image_ground_truth_for_keypoints=define_ground_truth_for_keypoints(image_ground_truth),
    )


def generate_smoke_data(dataset: SmokeDataset) -> Iterator[tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]]:
    yield (
        dataset.image.copy(),
        dataset.image_ground_truth.copy(),
        dataset.subset_has_image.copy(),
    )
