from __future__ import annotations

from datapipe.compute import DatapipeApp
from datapipe.datatable import DataStore
from datapipe.step.batch_transform import BatchTransform
from datapipe.types import Required
from datapipe_ml.metrics.model_selection import FindBestModel
from datapipe_ml.tasks.detection.freeze import DetectionFreezeDataset
from datapipe_ml.tasks.detection.inference import Inference_DetectionModel
from datapipe_ml.tasks.detection.train.yolov8 import Train_YoloV8_DetectionModel, YoloV8_TrainingConfig
from datapipe_ml.training.specs import TrainingResumeConfig, TrainingSyncConfig
from datapipe_ml.training.train_config_id import TrainingConfigPreset
from datapipe_ml.workflows.detection_classification.metrics import CountMetrics_Subset_PipelineModel

import steps
from config import (
    DATAPIPE_DIR,
    DBCONN,
    datapipe_tmp_folder,
    gpu_executor,
    metrics_executor,
    parallel_io_executor,
)
from data import catalog

# Data is loaded via the `load` step: add a row to `load_request` (request_id, n, offset, tag,
# darken) and run `datapipe step --labels=stage=load run`. It downloads COCO cat/dog images,
# uploads them to object storage, and produces s3_images + ground truth (+ tag) directly — there
# is no Label Studio annotation stage.

pipeline = [
    BatchTransform(
        func=steps.load_batch,
        inputs=["load_request"],
        outputs=["s3_images", "image__ground_truth", "tag", "image__tag", "image__subset_hint"],
        transform_keys=["request_id"],
        executor_config=parallel_io_executor(parallelism_cap=32),
        labels=[("stage", "load")],
    ),
    BatchTransform(
        func=steps.split_df_train_val,
        inputs=["image__ground_truth", "image__subset", "image__subset_hint"],
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
        labels=[("stage", "train"), ("stage", "train-prepare")]
    ),
    Train_YoloV8_DetectionModel(  # type: ignore[list-item]
        input__detection_frozen_dataset="detection_frozen_dataset",
        input__detection_frozen_dataset__has__image_gt="detection_frozen_dataset__has__image_gt",
        output__yolov8_train_config="yolov8_train_config",
        output__yolov8_custom_train_config="yolov8_custom_train_config",
        output__detection_training_request="detection_training_request",
        output__model_detection_size_for_resize="model_detection_size_for_resize",
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
            TrainingConfigPreset(
                name="Standard YOLOv8n 320",
                description="Default YOLOv8n detection preset for tags demo",
                config=YoloV8_TrainingConfig(
                    model="yolov8n.pt",
                    imgsz=320,
                    batch=32,
                    epochs=5,
                    freeze=10,
                    exist_ok=True,
                    seed=42,
                    workers=0
                ),
            )
        ],
        sync_config=TrainingSyncConfig(enabled=True, interval_s=30, retries=3, retry_sleep_s=30),
        resume_config=TrainingResumeConfig(
            continue_train_failed_models=True, min_completed_epochs=1, checkpoint="last",
            max_attempts=10, reset_attempts_after="10m", lease_ttl_s=60, heartbeat_interval_s=10,
        ),
        primary_keys=["image_name"],
        bbox_id__name=None,
        labels=[("stage", "train"), ("stage", "train-without-freeze")],
        allow_sample_size_mismatch=True,
        model_suffix="_tags",
        prepare_data_executor_config=parallel_io_executor(),
    ),
    Inference_DetectionModel(
        input__image="s3_images",
        input__detection_model="detection_model_train",
        output__detection_prediction="detection_prediction_train",
        primary_keys=["image_name"],
        bbox_id__name=None,
        image__image_path__name="image_url",
        batch_size_default=64,
        chunk_size=1024,
        prediction_threshold=0.10,
        executor_config=gpu_executor(),
        labels=[("stage", "train"), ("stage", "train-without-freeze"), ("stage", "inference")],
    ),
    CountMetrics_Subset_PipelineModel(
        input__image__ground_truth="image__ground_truth",
        input__subset__has__image="image__subset",
        input__pipeline_prediction="detection_prediction_train",
        output__pipeline_model__metrics_on__image="pipeline_model__metrics_on_image",
        output__pipeline_model__metrics_by_cls_on__subset="pipeline_model__metrics_by_cls_on_subset",
        output__pipeline_model__metrics_on__subset="pipeline_model__metrics_on_subset",
        primary_keys=["image_name"],
        bbox_id__name=None,
        pipeline_model_primary_keys=["detection_model_id"],
        minimum_iou=0.5,
        executor_config=metrics_executor(),
        labels=[("stage", "train"), ("stage", "train-without-freeze"), ("stage", "count-metrics")],
    ),
    FindBestModel(
        input__model="detection_model_train",
        input__model__metrics_on__subset="pipeline_model__metrics_on_subset",
        output__attr__model__is_best="attr__detection_model__is_best",
        output__best_model="best_detection_model",
        subset_id="val",
        is_best__name="detection_model__is_best",
        primary_keys=["detection_model_id"],
        metric__name="calc__weighted_f1_score",
        func="max",
        group_by=None,
        labels=[("stage", "train"), ("stage", "train-without-freeze"), ("stage", "count-metrics")],
    ),
    CountMetrics_Subset_PipelineModel(
        input__image__ground_truth=["image__ground_truth", "image__tag"],
        input__subset__has__image="image__subset",
        input__pipeline_prediction="detection_prediction_train",
        output__pipeline_model__metrics_on__image="pipeline_model__metrics_by_tag_on_image",
        output__pipeline_model__metrics_by_cls_on__subset="pipeline_model__metrics_by_tag_by_cls_on_subset",
        output__pipeline_model__metrics_on__subset="pipeline_model__metrics_by_tag_on_subset",
        primary_keys=["image_name"],
        bbox_id__name=None,
        pipeline_model_primary_keys=["detection_model_id", "tag_id"],
        minimum_iou=0.5,
        executor_config=metrics_executor(),
        labels=[
            ("stage", "train"),
            ("stage", "train-without-freeze"),
            ("stage", "count-metrics"),
            ("stage", "tag-metrics"),
        ],
    ),
    # --- FiftyOne (stage=fiftyone): GT + baseline/retrained predictions, filter by tag_id ---
    BatchTransform(
        func=steps.download_images,
        inputs=["s3_images"],
        outputs=["local_images"],
        transform_keys=["image_name"],
        labels=[("stage", "fiftyone")],
        executor_config=parallel_io_executor(),
        kwargs=dict(
            image__image_path__name="image_url",
            image__local_image_path__name="local_path",
        ),
    ),
    BatchTransform(
        func=steps.publish_to_fiftyone,
        inputs=["local_images"],
        outputs=["fiftyone_images"],
        labels=[("stage", "fiftyone")],
        kwargs=dict(
            primary_keys=["image_name"],
            image__image_path__name="local_path",
        ),
    ),
    BatchTransform(
        func=steps.publish_to_fiftyone_ground_truth,
        inputs=[
            "local_images",
            Required("image__ground_truth"),
            "image__subset",
            "image__tag",
        ],
        outputs=["fiftyone_annotations"],
        labels=[("stage", "fiftyone")],
        kwargs=dict(
            primary_keys=["image_name"],
            image__image_path__name="local_path",
        ),
    ),
    BatchTransform(
        func=steps.publish_to_fiftyone_predictions_baseline,
        inputs=[Required("local_images"), Required("detection_prediction_train")],
        outputs=["fiftyone_predictions_model_a"],
        transform_keys=["image_name", "detection_model_id"],
        labels=[("stage", "fiftyone")],
        kwargs=dict(
            primary_keys=["image_name"],
            image__image_path__name="local_path",
        ),
    ),
    BatchTransform(
        func=steps.publish_to_fiftyone_predictions_retrained,
        inputs=[Required("local_images"), Required("detection_prediction_train")],
        outputs=["fiftyone_predictions_model_b"],
        transform_keys=["image_name", "detection_model_id"],
        labels=[("stage", "fiftyone")],
        kwargs=dict(
            primary_keys=["image_name"],
            image__image_path__name="local_path",
        ),
    ),
]


ds = DataStore(DBCONN)
app = DatapipeApp(ds, catalog, pipeline)
