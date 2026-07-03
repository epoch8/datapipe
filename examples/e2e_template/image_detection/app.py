from __future__ import annotations

from datapipe.compute import Pipeline
from datapipe_app import DatapipeAPI
from datapipe.datatable import DataStore
from datapipe.step.batch_generate import BatchGenerate
from datapipe.step.batch_transform import BatchTransform
from datapipe_label_studio.upload_predictions_pipeline import LabelStudioUploadPredictions
from datapipe_label_studio.upload_tasks_pipeline import LabelStudioUploadTasks
from datapipe_ml.metrics.model_selection import FindBestModel
from datapipe_ml.training.specs import TrainingResumeConfig, TrainingSyncConfig
from datapipe_ml.tasks.detection.freeze import DetectionFreezeDataset
from datapipe_ml.tasks.detection.inference import Inference_DetectionModel
from datapipe_ml.workflows.detection_classification.metrics import CountMetrics_Subset_PipelineModel
from datapipe_ml.tasks.detection.train.yolov8 import Train_YoloV8_DetectionModel, YoloV8_TrainingConfig

import steps
from config import (
    DATAPIPE_DIR,
    DETECTION_MODEL_CONFIG,
    CLASSES_TO_KEEP,
    datapipe_tmp_folder,
    DBCONN,
    label_studio_storages,
    LABEL_CONFIG,
    LABEL_STUDIO_API_KEY,
    LABEL_STUDIO_URL,
    PROJECT_NAME,
)
from data import catalog

pipeline = Pipeline(
    [
        BatchGenerate(
            steps.list_s3_images,
            outputs=["s3_images"],
            labels=[("stage", "annotation")],
        ),
        BatchGenerate(
            steps.list_detection_models,
            outputs=["detection_model"],
            labels=[("stage", "annotation")],
            delete_stale=False,
        ),
        BatchTransform(
            func=steps.get_images_without_ground_truth,
            inputs=["s3_images", "image__ground_truth"],
            outputs=["sec__image_without_ground_truth"],
            transform_keys=["image_name"],
            kwargs=dict(primary_keys=["image_name"]),
            labels=[("stage", "annotation")],
        ),
        BatchTransform(
            func=steps.resolve_best_detection_model,
            inputs=["detection_model", "best_detection_model"],
            outputs=["best_detection_model"],
            transform_keys=["detection_model_id"],
            kwargs=dict(
                model_id_column="detection_model_id",
                fallback_model_id=DETECTION_MODEL_CONFIG["detection_model_id"],
            ),
            labels=[("stage", "annotation")],
        ),
        Inference_DetectionModel(
            input__image=["s3_images", "sec__image_without_ground_truth"],
            input__detection_model=["detection_model", "best_detection_model"],
            output__detection_prediction="ls_detection_prediction",
            primary_keys=["image_name"],
            bbox_id__name=None,
            image__image_path__name="image_url",
            batch_size_default=1,
            labels=[("stage", "annotation")],
        ),
        BatchTransform(
            func=steps.filter_bboxes_by_classes,
            inputs=["ls_detection_prediction"],
            outputs=["ls_detection_prediction"],
            transform_keys=["image_name", "detection_model_id"],
            labels=[("stage", "annotation")],
            kwargs=dict(
                classes_to_keep=CLASSES_TO_KEEP,
                primary_keys=["image_name"],
                model_id_column="detection_model_id",
            ),
        ),
        BatchTransform(
            func=steps.bboxes_to_ls_prediction,
            inputs=["ls_detection_prediction", "s3_images"],
            outputs=["images_with_predictions"],
            transform_keys=["image_name", "detection_model_id"],
            labels=[("stage", "annotation")],
            kwargs=dict(
                image__image_path__name="image_url",
                primary_keys=["image_name"],
                model_keys=["detection_model_id"],
            ),
        ),
        LabelStudioUploadTasks(
            input__item="s3_images",
            output__label_studio_project_task="ls_task",
            output__label_studio_project_annotation="ls_annotations",
            output__label_studio_sync_table="ls_sync",
            ls_url=LABEL_STUDIO_URL,
            api_key=LABEL_STUDIO_API_KEY,
            project_identifier=PROJECT_NAME,
            project_label_config_at_create=LABEL_CONFIG,
            primary_keys=["image_name"],
            columns=["image_url"],
            storages=label_studio_storages(),
            chunk_size=100,
            labels=[("stage", "annotation")],
        ),
        LabelStudioUploadPredictions(
            input__item__has__prediction="images_with_predictions",
            input__label_studio_project_task="ls_task",
            input__best_model="best_detection_model",
            output__label_studio_project_prediction="ls_predictions",
            output__label_studio_current_model_version="ls_current_model_version",
            ls_url=LABEL_STUDIO_URL,
            api_key=LABEL_STUDIO_API_KEY,
            project_identifier=PROJECT_NAME,
            primary_keys=["image_name", "detection_model_id"],
            model_keys=["detection_model_id"],
            labels=[("stage", "annotation")],
        ),
        BatchTransform(
            func=steps.parse_annotations_from_label_studio,
            inputs=["ls_annotations"],
            outputs=["image__ground_truth"],
            labels=[("stage", "annotation")],
            transform_keys=["image_name"],
        ),
        BatchTransform(
            func=steps.split_df_train_val,
            inputs=["image__ground_truth", "image__subset"],
            outputs=["image__subset"],
            labels=[("stage", "train"), ("stage", "train-prepare")],
            transform_keys=["image_name"],
            kwargs=dict(
                primary_keys=["image_name"],
                val_perc=0.25,
                random_seed=42,
            ),
        ),
        DetectionFreezeDataset(  # type: ignore[list-item]
            input__image="s3_images",
            input__image__ground_truth="image__ground_truth",
            input__subset__has__image="image__subset",
            output__detection_frozen_dataset="detection_frozen_dataset",
            output__detection_frozen_dataset__has__image_gt="detection_frozen_dataset__has__image_gt",
            working_dir=str(DATAPIPE_DIR),
            min_within_time="1s",
            min_delta=1,
            primary_keys=["image_name"],
            bbox_id__name=None,
            image__image_path__name="image_url",
            labels=[("stage", "train"), ("stage", "train-prepare")],
        ),
        Train_YoloV8_DetectionModel(  # type: ignore[list-item]
            input__detection_frozen_dataset="detection_frozen_dataset",
            input__detection_frozen_dataset__has__image_gt="detection_frozen_dataset__has__image_gt",
            output__yolov8_train_config="yolov8_train_config",
            output__detection_size_for_resize="detection_size_for_resize",
            output__detection_frozen_dataset__resized_image_file="detection_frozen_dataset__resized_image_file",
            output__detection_frozen_dataset__yolo_txt="detection_frozen_dataset__yolo_txt",
            output__detection_model="detection_model",
            output__detection_model_is_trained_on_detection_frozen_dataset=(
                "detection_model_is_trained_on_detection_frozen_dataset"
            ),
            output__training_status="detection_training_status",
            output__detection_frozen_dataset__class_names="detection_frozen_dataset__class_names",
            max_within_time="1w",
            working_dir=str(DATAPIPE_DIR),
            tmp_folder=datapipe_tmp_folder(),
            yolov8_train_configs=[
                YoloV8_TrainingConfig(
                    model="yolov8n.pt",
                    imgsz=320,
                    batch=10,
                    epochs=30,
                    exist_ok=True,
                )
            ],
            sync_config=TrainingSyncConfig(
                enabled=True,
                interval_s=30,
                retries=3,
                retry_sleep_s=30,
            ),
            resume_config=TrainingResumeConfig(
                continue_train_failed_models=True,
                min_completed_epochs=1,
                checkpoint="last",
                max_attempts=10,
                reset_attempts_after="10m",
                lease_ttl_s=60,
                heartbeat_interval_s=10,
            ),
            primary_keys=["image_name"],
            bbox_id__name=None,
            labels=[("stage", "train"), ("stage", "train-yolo")],
            allow_sample_size_mismatch=True,
            model_suffix="_e2e",
        ),
        Inference_DetectionModel(
            input__image=["s3_images", "image__subset"],
            input__detection_model="detection_model",
            output__detection_prediction="detection_prediction_raw",
            primary_keys=["image_name"],
            bbox_id__name=None,
            labels=[("stage", "train"), ("stage", "inference")],
            image__image_path__name="image_url",
            batch_size_default=1,
            filters={"subset_id": "val"},
        ),
        BatchTransform(
            func=steps.filter_bboxes_by_classes,
            inputs=["detection_prediction_raw"],
            outputs=["detection_prediction"],
            transform_keys=["image_name", "detection_model_id"],
            labels=[("stage", "train"), ("stage", "inference")],
            kwargs=dict(
                classes_to_keep=CLASSES_TO_KEEP,
                primary_keys=["image_name"],
                model_id_column="detection_model_id",
            ),
        ),
        CountMetrics_Subset_PipelineModel(
            input__image__ground_truth="image__ground_truth",
            input__subset__has__image="image__subset",
            input__pipeline_prediction="detection_prediction",
            output__pipeline_model__metrics_on__image="pipeline_model__metrics_on_image",
            output__pipeline_model__metrics_by_cls_on__subset="pipeline_model__metrics_by_cls_on_subset",
            output__pipeline_model__metrics_on__subset="pipeline_model__metrics_on_subset",
            primary_keys=["image_name"],
            bbox_id__name=None,
            pipeline_model_primary_keys=["detection_model_id"],
            labels=[("stage", "train"), ("stage", "count-metrics")],
            minimum_iou=0.5,
            filters={"subset_id": "val"},
        ),
        FindBestModel(
            input__model="detection_model",
            input__model__metrics_on__subset="pipeline_model__metrics_on_subset",
            output__attr__model__is_best="attr__detection_model__is_best",
            output__best_model="best_detection_model",
            subset_id="val",
            is_best__name="detection_model__is_best",
            primary_keys=["detection_model_id"],
            metric__name="calc__weighted_f1_score",
            func="max",
            group_by=None,
            labels=[("stage", "train"), ("stage", "count-metrics")],
        ),
        BatchTransform(
            func=steps.download_images,
            inputs=["s3_images"],
            outputs=["local_images"],
            transform_keys=["image_name"],
            labels=[("stage", "fiftyone")],
            kwargs=dict(
                image__image_path__name="image_url",
                image__local_image_path__name="local_path",
            ),
        ),
        BatchTransform(
            func=steps.publish_to_fiftyone,
            inputs=["local_images", "detection_prediction"],
            outputs=["fiftyone_predictions"],
            labels=[("stage", "fiftyone")],
            kwargs=dict(primary_keys=["image_name"], image__image_path__name="local_path"),
        ),
        BatchTransform(
            func=steps.publish_to_fiftyone,
            inputs=["local_images", "image__ground_truth"],
            outputs=["fiftyone_annotations"],
            labels=[("stage", "fiftyone")],
            kwargs=dict(primary_keys=["image_name"], image__image_path__name="local_path"),
        ),
    ]
)

ds = DataStore(DBCONN)
app = DatapipeAPI(ds, catalog, pipeline)
