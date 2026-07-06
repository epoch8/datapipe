from __future__ import annotations

from datapipe.compute import DatapipeApp, Pipeline
from datapipe.datatable import DataStore
from datapipe.step.batch_transform import BatchTransform
from datapipe_ml.metrics.model_selection import FindBestModel
from datapipe_ml.tasks.detection.freeze import DetectionFreezeDataset
from datapipe_ml.tasks.detection.inference import Inference_DetectionModel
from datapipe_ml.tasks.detection.metrics import CountMetrics_Subset_DetectionModel
from datapipe_ml.tasks.detection.train.yolov8 import Train_YoloV8_DetectionModel, YoloV8_TrainingConfig
from datapipe_ml.training.specs import TrainingResumeConfig, TrainingSyncConfig

import steps
from config import DATAPIPE_DIR, DBCONN, datapipe_tmp_folder
from data import catalog

# Data is loaded via the `load` step: add a row to `load_request` (request_id, n, offset, tag,
# darken) and run `datapipe step --labels=stage=load run`. It downloads COCO cat/dog images,
# uploads them to object storage, and produces s3_images + ground truth (+ tag) directly — there
# is no Label Studio annotation stage.

pipeline = Pipeline(
    [
        BatchTransform(
            func=steps.load_batch,
            inputs=["load_request"],
            outputs=["s3_images", "image__ground_truth", "tag", "image__tag"],
            transform_keys=["request_id"],
            labels=[("stage", "load")],
        ),
        BatchTransform(
            func=steps.split_df_train_val,
            inputs=["image__ground_truth", "image__subset"],
            outputs=["image__subset"],
            transform_keys=["image_name"],
            kwargs=dict(primary_keys=["image_name"], val_perc=0.25, random_seed=42),
            labels=[("stage", "train"), ("stage", "train-prepare")],
        ),
        DetectionFreezeDataset(  # type: ignore[list-item]
            input__image="s3_images",
            input__image__ground_truth="image__ground_truth",
            input__subset__has__image="image__subset",
            output__detection_frozen_dataset="detection_frozen_dataset",
            output__detection_frozen_dataset__has__image_gt="detection_frozen_dataset__has__image_gt",
            working_dir=str(DATAPIPE_DIR),
            min_within_time="1s",
            min_delta=10,
            primary_keys=["image_name"],
            bbox_id__name=None,
            image__image_path__name="image_url",
            labels=[("stage", "train"), ("stage", "train-prepare")],
            create_table=True,
        ),
        Train_YoloV8_DetectionModel(  # type: ignore[list-item]
            input__detection_frozen_dataset="detection_frozen_dataset",
            input__detection_frozen_dataset__has__image_gt="detection_frozen_dataset__has__image_gt",
            output__yolov8_train_config="yolov8_train_config",
            output__detection_size_for_resize="detection_size_for_resize",
            output__detection_frozen_dataset__resized_image_file="detection_frozen_dataset__resized_image_file",
            output__detection_frozen_dataset__yolo_txt="detection_frozen_dataset__yolo_txt",
            output__detection_model="detection_model_train",
            output__detection_model_is_trained_on_detection_frozen_dataset=(
                "detection_model_is_trained_on_detection_frozen_dataset"
            ),
            output__training_status="detection_training_status",
            output__detection_frozen_dataset__class_names="detection_frozen_dataset__class_names",
            max_within_time="1w",
            working_dir=str(DATAPIPE_DIR),
            tmp_folder=datapipe_tmp_folder(),
            yolov8_train_configs=[
                YoloV8_TrainingConfig(model="yolov8n.pt", imgsz=320, batch=10, epochs=50, exist_ok=True)
            ],
            sync_config=TrainingSyncConfig(enabled=True, interval_s=30, retries=3, retry_sleep_s=30),
            resume_config=TrainingResumeConfig(
                continue_train_failed_models=True, min_completed_epochs=1, checkpoint="last",
                max_attempts=10, reset_attempts_after="10m", lease_ttl_s=60, heartbeat_interval_s=10,
            ),
            primary_keys=["image_name"],
            bbox_id__name=None,
            labels=[("stage", "train")],
            create_table=True,
            allow_sample_size_mismatch=True,
            model_suffix="_tags",
        ),
        Inference_DetectionModel(
            input__image="s3_images",
            input__detection_model="detection_model_train",
            output__detection_prediction="detection_prediction_train",
            primary_keys=["image_name"],
            bbox_id__name=None,
            image__image_path__name="image_url",
            batch_size_default=1,
            labels=[("stage", "train"), ("stage", "inference")],
            create_table=True,
        ),
        CountMetrics_Subset_DetectionModel(
            input__image__ground_truth="image__ground_truth",
            input__subset__has__image="image__subset",
            input__detection_prediction="detection_prediction_train",
            output__detection_model__metrics__on__image="detection_model_train__metrics_on_image",
            output__detection_model__metrics__on__subset="detection_model_train__metrics_on_subset",
            primary_keys=["image_name"],
            bbox_id__name=None,
            minimum_iou=0.5,
            labels=[("stage", "train"), ("stage", "count-metrics")],
            create_table=True,
        ),
        FindBestModel(
            input__model="detection_model_train",
            input__model__metrics_on__subset="detection_model_train__metrics_on_subset",
            output__attr__model__is_best="attr__detection_model__is_best",
            output__best_model="best_detection_model",
            subset_id="val",
            is_best__name="detection_model__is_best",
            primary_keys=["detection_model_id"],
            metric__name="calc__f1_score",
            func="max",
            group_by=None,
            labels=[("stage", "train"), ("stage", "count-metrics")],
        ),
        # tag arc: aggregate per-image metrics by (model, tag, subset)
        BatchTransform(
            func=steps.compute_tag_metrics,
            inputs=["detection_model_train__metrics_on_image", "image__tag"],
            outputs=["tag_metrics"],
            transform_keys=["detection_model_id"],
            labels=[("stage", "train"), ("stage", "count-metrics"), ("stage", "tag-metrics")],
        ),
    ]
)

ds = DataStore(DBCONN, create_meta_table=True)
app = DatapipeApp(ds, catalog, pipeline)
