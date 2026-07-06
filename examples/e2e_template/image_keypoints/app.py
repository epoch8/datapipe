from __future__ import annotations

from datapipe.compute import Pipeline
from datapipe_app import DatapipeAPI
from datapipe.datatable import DataStore
from datapipe.step.batch_generate import BatchGenerate
from datapipe.step.batch_transform import BatchTransform
from datapipe_label_studio.upload_predictions_pipeline import LabelStudioUploadPredictions
from datapipe_label_studio.upload_tasks_pipeline import LabelStudioUploadTasks
from datapipe_ml.metrics.model_selection import FindBestModel
from datapipe.types import Required
from datapipe_ml.training.specs import TrainingResumeConfig, TrainingSyncConfig
from datapipe_ml.tasks.keypoints.freeze import KeypointsFreezeDataset
from datapipe_ml.tasks.keypoints.inference import Inference_KeypointsModel
from datapipe_ml.tasks.keypoints.metrics import CountMetrics_FrozenDataset_KeypointsModel
from datapipe_ml.tasks.keypoints.train.yolov8 import Train_YoloV8_KeypointsModel, YoloV8_TrainingConfig

import steps
from config import (
    CLASSES_TO_KEEP,
    DATAPIPE_DIR,
    KEYPOINTS_MODEL_CONFIG,
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
            steps.list_keypoints_models,
            outputs=["keypoints_model"],
            labels=[("stage", "annotation")],
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
            func=steps.combine_keypoints_models,
            inputs=["keypoints_model", "keypoints_model_train"],
            outputs=["keypoints_models"],
            transform_keys=["keypoints_model_id"],
            kwargs=dict(model_id_column="keypoints_model_id"),
            labels=[("stage", "annotation")],
        ),
        BatchTransform(
            func=steps.resolve_best_keypoints_model,
            inputs=["keypoints_models", "best_keypoints_model"],
            outputs=["best_keypoints_model"],
            transform_keys=["keypoints_model_id"],
            kwargs=dict(
                model_id_column="keypoints_model_id",
                fallback_model_id=KEYPOINTS_MODEL_CONFIG["keypoints_model_id"],
            ),
            labels=[("stage", "annotation")],
        ),
        Inference_KeypointsModel(
            input__image=["s3_images", "sec__image_without_ground_truth"],
            input__keypoints_model=["keypoints_models", "best_keypoints_model"],
            output__keypoints_prediction="ls_keypoints_prediction_raw",
            primary_keys=["image_name"],
            bbox_id__name=None,
            image__image_path__name="image_url",
            batch_size_default=1,
            labels=[("stage", "annotation")],
        ),
        BatchTransform(
            func=steps.filter_bboxes_by_classes,
            inputs=["ls_keypoints_prediction_raw"],
            outputs=["ls_keypoints_prediction"],
            transform_keys=["image_name", "keypoints_model_id"],
            labels=[("stage", "annotation")],
            kwargs=dict(
                classes_to_keep=CLASSES_TO_KEEP,
                primary_keys=["image_name"],
                model_id_column="keypoints_model_id",
            ),
        ),
        BatchTransform(
            func=steps.keypoints_to_ls_prediction,
            inputs=["ls_keypoints_prediction", "s3_images"],
            outputs=["images_with_predictions"],
            transform_keys=["image_name", "keypoints_model_id"],
            labels=[("stage", "annotation")],
            kwargs=dict(
                image__image_path__name="image_url",
                primary_keys=["image_name"],
                model_keys=["keypoints_model_id"],
                hide_bboxes=True,
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
            input__best_model="best_keypoints_model",
            output__label_studio_project_prediction="ls_predictions",
            output__label_studio_current_model_version="ls_current_model_version",
            ls_url=LABEL_STUDIO_URL,
            api_key=LABEL_STUDIO_API_KEY,
            project_identifier=PROJECT_NAME,
            primary_keys=["image_name", "keypoints_model_id"],
            model_keys=["keypoints_model_id"],
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
        KeypointsFreezeDataset(  # type: ignore[list-item]
            input__image="s3_images",
            input__image__ground_truth="image__ground_truth",
            input__subset__has__image="image__subset",
            output__keypoints_frozen_dataset="keypoints_frozen_dataset",
            output__keypoints_frozen_dataset__has__image_gt="keypoints_frozen_dataset__has__image_gt",
            working_dir=str(DATAPIPE_DIR),
            min_within_time="1d",
            min_delta=10,
            primary_keys=["image_name"],
            bbox_id__name=None,
            image__image_path__name="image_url",
            labels=[("stage", "train"), ("stage", "train-prepare")],
        ),
        Train_YoloV8_KeypointsModel(  # type: ignore[list-item]
            input__keypoints_frozen_dataset="keypoints_frozen_dataset",
            input__keypoints_frozen_dataset__has__image_gt="keypoints_frozen_dataset__has__image_gt",
            output__yolov8_train_config="yolov8_train_config",
            output__keypoints_size_for_resize="keypoints_size_for_resize",
            output__keypoints_frozen_dataset__resized_image_file="keypoints_frozen_dataset__resized_image_file",
            output__keypoints_frozen_dataset__yolo_txt="keypoints_frozen_dataset__yolo_txt",
            output__keypoints_model="keypoints_model_train",
            output__keypoints_model_is_trained_on_keypoints_frozen_dataset=(
                "keypoints_model_is_trained_on_keypoints_frozen_dataset"
            ),
            output__training_status="keypoints_training_status",
            output__keypoints_frozen_dataset__class_names="keypoints_frozen_dataset__class_names",
            max_within_time="1w",
            working_dir=str(DATAPIPE_DIR),
            tmp_folder=datapipe_tmp_folder(),
            yolov8_train_configs=[
                YoloV8_TrainingConfig(
                    model="yolov8n-pose.pt",
                    imgsz=320,
                    batch=8,
                    epochs=30,
                    exist_ok=True,
                    save_period=1,
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
            labels=[("stage", "train")],
            allow_sample_size_mismatch=True,
            model_suffix="_e2e",
        ),
        Inference_KeypointsModel(
            input__image=["s3_images", "image__subset"],
            input__keypoints_model="keypoints_model_train",
            output__keypoints_prediction="keypoints_prediction_train_raw",
            primary_keys=["image_name"],
            bbox_id__name=None,
            labels=[("stage", "train"), ("stage", "inference")],
            image__image_path__name="image_url",
            batch_size_default=1,
            filters={"subset_id": "val"},
        ),
        BatchTransform(
            func=steps.filter_bboxes_by_classes,
            inputs=["keypoints_prediction_train_raw"],
            outputs=["keypoints_prediction_train"],
            transform_keys=["image_name", "keypoints_model_id"],
            labels=[("stage", "train"), ("stage", "inference")],
            kwargs=dict(
                classes_to_keep=CLASSES_TO_KEEP,
                primary_keys=["image_name"],
                model_id_column="keypoints_model_id",
            ),
        ),
        CountMetrics_FrozenDataset_KeypointsModel(
            input__keypoints_frozen_dataset__has__image_gt="keypoints_frozen_dataset__has__image_gt",
            input__keypoints_model="keypoints_model_train",
            input__keypoints_prediction="keypoints_prediction_train",
            output__keypoints_model__metrics_on__frozen_dataset="keypoints_model_train__metrics_on_frozen_dataset",
            primary_keys=["image_name"],
            bbox_id__name=None,
            labels=[("stage", "train"), ("stage", "count-metrics")],
            minimum_iou=0.5,
            filters={"subset_id": "val"},
        ),
        FindBestModel(
            input__model="keypoints_model_train",
            input__model__metrics_on__subset="keypoints_model_train__metrics_on_frozen_dataset",
            output__attr__model__is_best="attr__keypoints_model__is_best",
            output__best_model="best_keypoints_model",
            subset_id="val",
            is_best__name="keypoints_model__is_best",
            primary_keys=["keypoints_model_id"],
            metric__name="calc__pose_mAP50_95",
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
            inputs=["local_images", Required("image__ground_truth"), Required("image__subset")],
            outputs=["fiftyone_annotations"],
            labels=[("stage", "fiftyone")],
            kwargs=dict(
                primary_keys=["image_name"],
                image__image_path__name="local_path",
            ),
        ),
        BatchTransform(
            func=steps.publish_to_fiftyone_predictions_from_best_model,
            inputs=["local_images", "keypoints_prediction_train", Required("best_keypoints_model")],
            outputs=["fiftyone_predictions_from_best_model"],
            transform_keys=["image_name", "keypoints_model_id"],
            labels=[("stage", "fiftyone")],
            kwargs=dict(
                primary_keys=["image_name"],
                model_keys=["keypoints_model_id"],
                image__image_path__name="local_path",
            ),
        ),
    ]
)

ds = DataStore(DBCONN)
app = DatapipeAPI(ds, catalog, pipeline)
