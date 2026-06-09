from __future__ import annotations

import json
import os
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import pandas as pd
from datapipe.compute import (
    Catalog,
    Pipeline,
    PipelineStep,
    Table,
    build_compute,
    run_steps,
)
from datapipe.datatable import DataStore
from datapipe.store.database import DBConn, TableStoreDB
from sklearn.model_selection import train_test_split
from sqlalchemy import Column
from sqlalchemy.sql.sqltypes import JSON, String
from tests.conftest import get_sqlite_dbconnstr

TESTS_DIR = Path(__file__).parents[1]
INPUT_DIR = TESTS_DIR / "input"
IMAGES_DIR = INPUT_DIR / "images"
LABELS_DIR = INPUT_DIR / "labels"
KEYPOINTS_INPUT_DIR = TESTS_DIR / "input_kps"
KEYPOINTS_IMAGES_DIR = KEYPOINTS_INPUT_DIR / "images"
KEYPOINTS_LABELS_DIR = KEYPOINTS_INPUT_DIR / "labels"

SMOKE_IMAGES = 8
SMOKE_EPOCHS = int(os.environ.get("DATAPIPE_ML_SMOKE_EPOCHS", "2"))
SMOKE_IMGSZ = int(os.environ.get("DATAPIPE_ML_SMOKE_IMGSZ", "16"))
SMOKE_YOLOV5_IMGSZ = int(os.environ.get("DATAPIPE_ML_SMOKE_YOLOV5_IMGSZ", str(max(64, SMOKE_IMGSZ))))
SMOKE_DEVICE = os.environ.get("DATAPIPE_ML_SMOKE_DEVICE", "cpu")
PRIMARY_KEYS = ["image_id"]


@dataclass
class SmokeRuntime:
    ds: DataStore
    catalog: Catalog
    workdir: Path


def ensure_input_data(*, include_keypoints_gt: bool = False) -> None:
    has_images = IMAGES_DIR.exists() and any(IMAGES_DIR.glob("*.jpg"))
    has_labels = LABELS_DIR.exists() and any(LABELS_DIR.glob("*.json"))
    has_keypoints_images = KEYPOINTS_IMAGES_DIR.exists() and any(KEYPOINTS_IMAGES_DIR.glob("*.jpg"))
    has_keypoints_labels = KEYPOINTS_LABELS_DIR.exists() and any(KEYPOINTS_LABELS_DIR.glob("*.json"))
    has_requested_data = (
        has_images and has_labels and (not include_keypoints_gt or (has_keypoints_images and has_keypoints_labels))
    )
    if has_requested_data:
        return
    subprocess.run([sys.executable, str(TESTS_DIR / "download_input.py")], cwd=TESTS_DIR, check=True)


def load_keypoints_ground_truth(limit: int = SMOKE_IMAGES) -> tuple[pd.DataFrame, pd.DataFrame]:
    ensure_input_data(include_keypoints_gt=True)
    image_paths = sorted(KEYPOINTS_IMAGES_DIR.glob("*.jpg"))[:limit]
    label_paths = [KEYPOINTS_LABELS_DIR / f"{path.stem}.json" for path in image_paths]
    missing = [path for path in label_paths if not path.exists()]
    if missing:
        raise AssertionError(f"Missing keypoints labels for images: {missing[:3]}")

    labels_data = [json.loads(path.read_text()) for path in label_paths]
    return (
        pd.DataFrame(
            {
                "image_id": [path.stem for path in image_paths],
                "image__image_path": [str(path) for path in image_paths],
            }
        ),
        pd.DataFrame(
            {
                "image_id": [path.stem for path in image_paths],
                "bboxes": [item["bboxes"] for item in labels_data],
                "labels": [item["labels"] for item in labels_data],
                "keypoints": [item["keypoints"] for item in labels_data],
                "keypoints_visibility": [item["keypoints_visibility"] for item in labels_data],
                "flip_idx": [item["flip_idx"] for item in labels_data],
            }
        ),
    )


def load_training_frames(limit: int = SMOKE_IMAGES) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    ensure_input_data()
    image_paths = sorted(IMAGES_DIR.glob("*.jpg"))[:limit]
    label_paths = [LABELS_DIR / f"{path.stem}.json" for path in image_paths]
    missing = [path for path in label_paths if not path.exists()]
    if missing:
        raise AssertionError(f"Missing labels for images: {missing[:3]}")

    labels_data = [json.loads(path.read_text()) for path in label_paths]
    idxs = list(range(len(image_paths)))
    idxs_train, _ = train_test_split(idxs, test_size=0.25, random_state=42)
    train_set = set(idxs_train)
    return (
        pd.DataFrame(
            {
                "image_id": [path.stem for path in image_paths],
                "image__image_path": [str(path) for path in image_paths],
            }
        ),
        pd.DataFrame(
            {
                "image_id": [path.stem for path in image_paths],
                "bboxes": [item["bboxes"] for item in labels_data],
                "labels": [item["labels"] for item in labels_data],
                "masks": [item["masks"] for item in labels_data],
            }
        ),
        pd.DataFrame(
            {
                "image_id": [path.stem for path in image_paths],
                "subset_id": ["train" if idx in train_set else "val" for idx in idxs],
            }
        ),
    )


def make_runtime(
    tmp_path: Path, *, include_classification_gt: bool = False, include_keypoints_gt: bool = False
) -> SmokeRuntime:
    dbconn = DBConn(get_sqlite_dbconnstr(tmp_path / "training_smoke.sqlite"))
    ds = DataStore(dbconn, create_meta_table=True)
    catalog = build_base_catalog(
        dbconn,
        include_classification_gt=include_classification_gt,
        include_keypoints_gt=include_keypoints_gt,
    )
    keypoints_gt = None
    if include_keypoints_gt:
        image, keypoints_gt = load_keypoints_ground_truth()
        idxs = list(range(len(image)))
        idxs_train, _ = train_test_split(idxs, test_size=0.25, random_state=42)
        train_set = set(idxs_train)
        subset = pd.DataFrame(
            {
                "image_id": image["image_id"],
                "subset_id": ["train" if idx in train_set else "val" for idx in idxs],
            }
        )
        gt = pd.DataFrame(
            {
                "image_id": image["image_id"],
                "bboxes": [[] for _ in range(len(image))],
                "labels": [[] for _ in range(len(image))],
                "masks": [[] for _ in range(len(image))],
            }
        )
    else:
        image, gt, subset = load_training_frames()
    store_frames(ds, catalog, (image, gt, subset))
    if include_classification_gt:
        classification_gt = pd.DataFrame(
            {
                "image_id": image["image_id"],
                "label": [f"smoke_class_{idx % 2}" for idx in range(len(image))],
            }
        )
        catalog.get_datatable(ds, "image__ground_truth_for_classification").store_chunk(classification_gt)
    if include_keypoints_gt:
        assert keypoints_gt is not None
        catalog.get_datatable(ds, "image__ground_truth_for_keypoints").store_chunk(keypoints_gt)
    return SmokeRuntime(ds=ds, catalog=catalog, workdir=tmp_path)


def build_base_catalog(
    dbconn: DBConn, *, include_classification_gt: bool = False, include_keypoints_gt: bool = False
) -> Catalog:
    tables = {
        "image": Table(
            store=TableStoreDB(
                dbconn,
                "image",
                [Column("image_id", String, primary_key=True), Column("image__image_path", String)],
                True,
            )
        ),
        "image__ground_truth": Table(
            store=TableStoreDB(
                dbconn,
                "image__ground_truth",
                [
                    Column("image_id", String, primary_key=True),
                    Column("bboxes", JSON),
                    Column("labels", JSON),
                    Column("masks", JSON),
                ],
                True,
            )
        ),
        "subset__has__image": Table(
            store=TableStoreDB(
                dbconn,
                "subset__has__image",
                [Column("image_id", String, primary_key=True), Column("subset_id", String, primary_key=True)],
                True,
            )
        ),
    }
    if include_classification_gt:
        tables["image__ground_truth_for_classification"] = Table(
            store=TableStoreDB(
                dbconn,
                "image__ground_truth_for_classification",
                [Column("image_id", String, primary_key=True), Column("label", String)],
                True,
            )
        )
    if include_keypoints_gt:
        tables["image__ground_truth_for_keypoints"] = Table(
            store=TableStoreDB(
                dbconn,
                "image__ground_truth_for_keypoints",
                [
                    Column("image_id", String, primary_key=True),
                    Column("bboxes", JSON),
                    Column("labels", JSON),
                    Column("keypoints", JSON),
                    Column("keypoints_visibility", JSON),
                    Column("flip_idx", JSON),
                ],
                True,
            )
        )
    return Catalog(tables)


def store_frames(ds: DataStore, catalog: Catalog, frames: Iterable[pd.DataFrame]) -> None:
    for table_name, frame in zip(["image", "image__ground_truth", "subset__has__image"], frames):
        catalog.get_datatable(ds, table_name).store_chunk(frame)


def run_pipeline(runtime: SmokeRuntime, steps: Iterable[PipelineStep]) -> None:
    compute_steps = build_compute(runtime.ds, runtime.catalog, Pipeline(list(steps)))
    run_steps(runtime.ds, compute_steps)


def assert_model_artifact(
    runtime: SmokeRuntime, table_name: str, type_column: str, path_column: str, expected_type: str
):
    df_model = runtime.ds.get_table(table_name).get_data()
    assert len(df_model) == 1
    assert df_model[type_column].iloc[0] == expected_type
    assert Path(df_model[path_column].iloc[0]).exists()


def assert_table_has_rows(runtime: SmokeRuntime, table_name: str) -> pd.DataFrame:
    df = runtime.ds.get_table(table_name).get_data()
    assert len(df) > 0
    return df


def assert_metrics_have_values(runtime: SmokeRuntime, table_name: str, metric_columns: Iterable[str]) -> pd.DataFrame:
    df = assert_table_has_rows(runtime, table_name)
    missing_columns = [column for column in metric_columns if column not in df.columns]
    assert missing_columns == []
    for column in metric_columns:
        assert df[column].notna().any(), f"{table_name}.{column} has no non-null values"
    if "calc__support" in df.columns:
        assert df["calc__support"].fillna(0).sum() > 0
    return df


def detection_freeze_step(workdir: Path):
    from datapipe_ml.tasks.detection.freeze import DetectionFreezeDataset

    return DetectionFreezeDataset(
        input__image="image",
        input__image__ground_truth="image__ground_truth",
        input__subset__has__image="subset__has__image",
        output__detection_frozen_dataset="detection_frozen_dataset",
        output__detection_frozen_dataset__has__image_gt="detection_frozen_dataset__has__image_gt",
        working_dir=str(workdir),
        primary_keys=PRIMARY_KEYS,
        min_delta=1,
        min_within_time="0s",
        create_table=True,
        bbox_id__name=None,
    )


def detection_train_step(workdir: Path):
    from datapipe_ml.tasks.detection.train.yolov8 import (
        Train_YoloV8_DetectionModel,
        YoloV8_TrainingConfig,
    )

    return Train_YoloV8_DetectionModel(
        input__detection_frozen_dataset="detection_frozen_dataset",
        input__detection_frozen_dataset__has__image_gt="detection_frozen_dataset__has__image_gt",
        output__yolov8_train_config="yolov8_train_config",
        output__detection_size_for_resize="yolov8_detection_size_for_resize",
        output__detection_frozen_dataset__resized_image_file="yolov8_detection_resized_image_file",
        output__detection_frozen_dataset__yolo_txt="yolov8_detection_yolo_txt",
        output__detection_frozen_dataset__class_names="yolov8_detection_class_names",
        output__detection_model="detection_model",
        output__detection_model_is_trained_on_detection_frozen_dataset="detection_model_link",
        working_dir=str(workdir),
        yolov8_train_configs=[
            YoloV8_TrainingConfig(
                model="yolo11n.pt",
                imgsz=SMOKE_IMGSZ,
                batch=2,
                epochs=SMOKE_EPOCHS,
                seed=42,
                device=SMOKE_DEVICE,
                workers=0,
                patience=SMOKE_EPOCHS,
                amp=False,
                val=False,
                plots=False,
            )
        ],
        primary_keys=PRIMARY_KEYS,
        create_table=True,
        ignore_errors_sample_sizes=True,
        tmp_folder=str(workdir / "tmp"),
        model_suffix="_smoke",
    )


def detection_yolov5_train_step(workdir: Path):
    from datapipe_ml.tasks.detection.train.yolov5 import (
        Train_YoloV5_DetectionModel,
        YoloV5_TrainingConfig,
    )

    return Train_YoloV5_DetectionModel(
        input__detection_frozen_dataset="detection_frozen_dataset",
        input__detection_frozen_dataset__has__image_gt="detection_frozen_dataset__has__image_gt",
        output__yolov5_train_config="yolov5_train_config",
        output__detection_size_for_resize="yolov5_detection_size_for_resize",
        output__detection_frozen_dataset__resized_image_file="yolov5_detection_resized_image_file",
        output__detection_frozen_dataset__yolo_txt="yolov5_detection_yolo_txt",
        output__detection_frozen_dataset__class_names="yolov5_detection_class_names",
        output__detection_model="detection_model",
        output__detection_model_is_trained_on_detection_frozen_dataset="detection_model_link",
        working_dir=str(workdir),
        yolov5_train_configs=[
            YoloV5_TrainingConfig(
                weights="",
                cfg="yolov5.ROOT / 'models/yolov5n.yaml'",
                imgsz=SMOKE_YOLOV5_IMGSZ,
                batch_size=2,
                epochs=SMOKE_EPOCHS,
                seed=42,
                device=SMOKE_DEVICE,
                workers=0,
                patience=SMOKE_EPOCHS,
                noautoanchor=True,
                noplots=True,
            )
        ],
        primary_keys=PRIMARY_KEYS,
        create_table=True,
        ignore_errors_sample_sizes=True,
        tmp_folder=str(workdir / "tmp"),
        model_suffix="_smoke",
    )


def detection_inference_step():
    from datapipe_ml.tasks.detection.inference import Inference_DetectionModel

    return Inference_DetectionModel(
        input__image="image",
        input__detection_model="detection_model",
        output__detection_prediction="detection_prediction",
        primary_keys=PRIMARY_KEYS,
        chunk_size=2,
        create_table=True,
        bbox_id__name=None,
        batch_size_default=1,
        prediction_threshold=0.01,
    )


def detection_metrics_step():
    from datapipe_ml.tasks.detection.metrics import CountMetrics_Subset_DetectionModel

    return CountMetrics_Subset_DetectionModel(
        input__image__ground_truth="image__ground_truth",
        input__subset__has__image="subset__has__image",
        input__detection_prediction="detection_prediction",
        output__detection_model__metrics__on__image="detection_metrics_on_image",
        output__detection_model__metrics__on__subset="detection_metrics_on_subset",
        primary_keys=PRIMARY_KEYS,
        bbox_id__name=None,
        create_table=True,
    )


def segmentation_freeze_step(workdir: Path):
    from datapipe_ml.tasks.segmentation.freeze import SegmentationFreezeDataset

    return SegmentationFreezeDataset(
        input__image="image",
        input__image__ground_truth="image__ground_truth",
        input__subset__has__image="subset__has__image",
        output__segmentation_frozen_dataset="segmentation_frozen_dataset",
        output__segmentation_frozen_dataset__has__image_gt="segmentation_frozen_dataset__has__image_gt",
        working_dir=str(workdir),
        primary_keys=PRIMARY_KEYS,
        min_delta=1,
        min_within_time="0s",
        create_table=True,
        bbox_id__name=None,
    )


def segmentation_train_step(workdir: Path):
    from datapipe_ml.tasks.segmentation.train.yolov8 import (
        Train_YoloV8_SegmentationModel,
        YoloV8_TrainingConfig,
    )

    return Train_YoloV8_SegmentationModel(
        input__segmentation_frozen_dataset="segmentation_frozen_dataset",
        input__segmentation_frozen_dataset__has__image_gt="segmentation_frozen_dataset__has__image_gt",
        output__yolov8_train_config="segmentation_yolov8_train_config",
        output__segmentation_size_for_resize="segmentation_size_for_resize",
        output__segmentation_frozen_dataset__class_names="segmentation_class_names",
        output__segmentation_frozen_dataset__resized_image_file="segmentation_resized_image_file",
        output__segmentation_frozen_dataset__yolo_txt="segmentation_yolo_txt",
        output__segmentation_model="segmentation_model",
        output__segm_model_is_trained_on_segm_frozen_dataset="segmentation_model_link",
        working_dir=str(workdir),
        yolov8_train_configs=[
            YoloV8_TrainingConfig(
                model="yolov8n-seg.pt",
                imgsz=SMOKE_IMGSZ,
                batch=2,
                epochs=SMOKE_EPOCHS,
                seed=42,
                device=SMOKE_DEVICE,
                workers=0,
                patience=SMOKE_EPOCHS,
                amp=False,
                val=False,
                plots=False,
            )
        ],
        primary_keys=PRIMARY_KEYS,
        create_table=True,
        tmp_folder=str(workdir / "tmp"),
        model_suffix="_smoke",
    )


def segmentation_inference_step():
    from datapipe_ml.tasks.segmentation.inference import Inference_SegmentationModel

    return Inference_SegmentationModel(
        input__image="image",
        input__segmentation_model="segmentation_model",
        output__segmentation_prediction="segmentation_prediction",
        primary_keys=PRIMARY_KEYS,
        chunk_size=2,
        create_table=True,
        bbox_id__name=None,
        batch_size_default=1,
        prediction_threshold=0.01,
    )


def keypoints_freeze_step(workdir: Path):
    from datapipe_ml.tasks.keypoints.freeze import KeypointsFreezeDataset

    return KeypointsFreezeDataset(
        input__image="image",
        input__image__ground_truth="image__ground_truth_for_keypoints",
        input__subset__has__image="subset__has__image",
        output__keypoints_frozen_dataset="keypoints_frozen_dataset",
        output__keypoints_frozen_dataset__has__image_gt="keypoints_frozen_dataset__has__image_gt",
        working_dir=str(workdir),
        primary_keys=PRIMARY_KEYS,
        min_delta=1,
        min_within_time="0s",
        create_table=True,
        bbox_id__name=None,
    )


def keypoints_train_step(workdir: Path):
    from datapipe_ml.tasks.keypoints.train.yolov8 import (
        Train_YoloV8_KeypointsModel,
        YoloV8_TrainingConfig,
    )

    return Train_YoloV8_KeypointsModel(
        input__keypoints_frozen_dataset="keypoints_frozen_dataset",
        input__keypoints_frozen_dataset__has__image_gt="keypoints_frozen_dataset__has__image_gt",
        output__yolov8_train_config="keypoints_yolov8_train_config",
        output__keypoints_size_for_resize="keypoints_size_for_resize",
        output__keypoints_frozen_dataset__class_names="keypoints_class_names",
        output__keypoints_frozen_dataset__resized_image_file="keypoints_resized_image_file",
        output__keypoints_frozen_dataset__yolo_txt="keypoints_yolo_txt",
        output__keypoints_model="keypoints_model",
        output__keypoints_model_is_trained_on_keypoints_frozen_dataset="keypoints_model_link",
        working_dir=str(workdir),
        yolov8_train_configs=[
            YoloV8_TrainingConfig(
                model="yolo11n-pose.pt",
                imgsz=SMOKE_IMGSZ,
                batch=2,
                epochs=SMOKE_EPOCHS,
                seed=42,
                device=SMOKE_DEVICE,
                workers=0,
                patience=SMOKE_EPOCHS,
                amp=False,
                val=False,
                plots=False,
            )
        ],
        primary_keys=PRIMARY_KEYS,
        create_table=True,
        bbox_id__name=None,
        ignore_errors_sample_sizes=True,
        tmp_folder=str(workdir / "tmp"),
        model_suffix="_smoke",
    )


def keypoints_inference_step():
    from datapipe_ml.tasks.keypoints.inference import Inference_KeypointsModel

    return Inference_KeypointsModel(
        input__image="image",
        input__keypoints_model="keypoints_model",
        output__keypoints_prediction="keypoints_prediction",
        primary_keys=PRIMARY_KEYS,
        chunk_size=2,
        create_table=True,
        bbox_id__name=None,
        batch_size_default=1,
        prediction_threshold=0.01,
    )


def keypoints_metrics_step():
    from datapipe_ml.tasks.keypoints.metrics import CountMetrics_Subset_KeypointsModel

    return CountMetrics_Subset_KeypointsModel(
        input__image__ground_truth="image__ground_truth_for_keypoints",
        input__subset__has__image="subset__has__image",
        input__keypoints_model="keypoints_model",
        input__keypoints_prediction="keypoints_prediction",
        output__keypoints_model__metrics__on__subset="keypoints_metrics_on_subset",
        primary_keys=PRIMARY_KEYS,
        bbox_id__name=None,
        create_table=True,
        yolo_validation_batch=1,
        yolo_validation_device=SMOKE_DEVICE,
    )


def classification_freeze_step(workdir: Path):
    from datapipe_ml.tasks.classification.freeze import ClassificationFreezeDataset

    return ClassificationFreezeDataset(
        input__image="image",
        input__image__ground_truth="image__ground_truth_for_classification",
        input__subset__has__image="subset__has__image",
        output__classification_frozen_dataset="classification_frozen_dataset",
        output__classification_frozen_dataset__has__image_gt="classification_frozen_dataset__has__image_gt",
        working_dir=str(workdir),
        primary_keys=PRIMARY_KEYS,
        min_delta=1,
        min_within_time="0s",
        create_table=True,
    )


def classification_train_step(workdir: Path):
    from datapipe_ml.tasks.classification.train.tensorflow import (
        TF_ClassificationTrainingConfig,
        Train_Tensorflow_ClassificationModel,
    )

    return Train_Tensorflow_ClassificationModel(
        input__classification_frozen_dataset="classification_frozen_dataset",
        input__classification_frozen_dataset__has__image_gt="classification_frozen_dataset__has__image_gt",
        output__tf_classification_train_config="tf_classification_train_config",
        output__classification_model="classification_model",
        output__classification_model_is_trained_on_cls_frozen_dataset="classification_model_link",
        working_dir=str(workdir),
        tf_classification_train_configs=[
            TF_ClassificationTrainingConfig(
                image_size=(SMOKE_IMGSZ, SMOKE_IMGSZ),
                seed=42,
                batch_size=1,
                arch="tiny_cnn",
                init_lr=0.001,
                reduce_lr_patience=1,
                reduce_lr_factor=0.5,
                early_stopping_patience=SMOKE_EPOCHS,
                epochs=SMOKE_EPOCHS,
                label_smoothing=0.0,
                augmentations=False,
                augment_func_file=None,
                class_weight=False,
            )
        ],
        primary_keys=PRIMARY_KEYS,
        create_table=True,
        tmp_folder=str(workdir / "tmp"),
        model_suffix="_smoke",
        clean_checkpoints_after_train=True,
    )


def classification_inference_step():
    from datapipe_ml.tasks.classification.inference import Inference_ClassificationModel

    return Inference_ClassificationModel(
        input__image="image",
        input__classification_model="classification_model",
        output__classification_prediction="classification_prediction",
        primary_keys=PRIMARY_KEYS,
        chunk_size=2,
        create_table=True,
        batch_size_default=1,
    )


def classification_metrics_step():
    from datapipe_ml.tasks.classification.metrics import (
        CountMetrics_Subset_ClassificationModel,
    )

    return CountMetrics_Subset_ClassificationModel(
        input__image__ground_truth="image__ground_truth_for_classification",
        input__subset__has__image="subset__has__image",
        input__classification_prediction="classification_prediction",
        output__classification_model__metrics__on__image="classification_metrics_on_image",
        output__classification_model__metrics_by_cls_on__subset="classification_metrics_by_cls_on_subset",
        output__classification_model__metrics_on__subset="classification_metrics_on_subset",
        primary_keys=PRIMARY_KEYS,
        chunk_size=2,
        create_table=True,
    )
