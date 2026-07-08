from __future__ import annotations

from datapipe.compute import Pipeline
from datapipe_app import (
    DatapipeAPI,
    DatapipeOpsSpec,
    OpsClassMetricTableSpec,
    OpsColumn,
    OpsColumnGroup,
    OpsDataSpec,
    OpsFilterRule,
    OpsFrozenDatasetSpec,
    OpsMetricTableSpec,
    OpsModelSpec,
    OpsRelationSpec,
    OpsTrainingSpec,
)
from datapipe.datatable import DataStore
from datapipe.step.batch_generate import BatchGenerate
from datapipe.step.batch_transform import BatchTransform
from datapipe_label_studio.upload_predictions_pipeline import LabelStudioUploadPredictions
from datapipe_label_studio.upload_tasks_pipeline import LabelStudioUploadTasks
from datapipe_ml.metrics.model_selection import FindBestModel
from datapipe.types import Required
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
            output__detection_prediction="ls_detection_prediction_raw",
            primary_keys=["image_name"],
            bbox_id__name=None,
            image__image_path__name="image_url",
            batch_size_default=1,
            labels=[("stage", "annotation")],
        ),
        BatchTransform(
            func=steps.filter_bboxes_by_classes,
            inputs=["ls_detection_prediction_raw"],
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
            min_delta=10,
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
            # filters={"subset_id": "val"},
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
            # filters={"subset_id": "val"},
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
            inputs=["local_images", "detection_prediction", Required("best_detection_model")],
            outputs=["fiftyone_predictions_from_best_model"],
            transform_keys=["image_name", "detection_model_id"],
            labels=[("stage", "fiftyone")],
            kwargs=dict(
                primary_keys=["image_name"],
                model_keys=["detection_model_id"],
                image__image_path__name="local_path",
            ),
        ),
    ]
)

ds = DataStore(DBCONN)
app = DatapipeAPI(ds, catalog, pipeline)

app.add_specs([
    DatapipeOpsSpec(
        id="cat_dog_smoke_yolo",
        title="Cat / Dog Smoke YOLO",
        description="YOLO training pipeline over frozen image snapshots.",
        icon="shield",
        color="blue",
        data=OpsDataSpec(
            tables=[
                "s3_images",
                "image__ground_truth",
                "image__subset",
                "detection_frozen_dataset",
                "detection_model",
                "pipeline_model__metrics_on_subset",
                "pipeline_model__metrics_by_cls_on_subset",
            ],
            item_table="s3_images",
            label_table="image__ground_truth",
            subset_table="image__subset",
        ),
        frozen_dataset=OpsFrozenDatasetSpec(
            table="detection_frozen_dataset",
            id_column="detection_frozen_dataset_id",
            created_at_column="detection_frozen_dataset__created_at",
            label_mode="timestamp",
            split_columns={
                "train": "detection_frozen_dataset__train_images_count",
                "val": "detection_frozen_dataset__val_images_count",
                "test": "detection_frozen_dataset__test_images_count",
            },
            models_count_relation_id="model_trained_on_frozen_dataset",
        ),
        model=OpsModelSpec(
            table="detection_model",
            id_column="detection_model_id",
            artifact_uri_column="detection_model__model_path",
            is_best_table="attr__detection_model__is_best",
            is_best_column="detection_model__is_best",
        ),
        training=OpsTrainingSpec(
            status_table="detection_training_status",
            model_id_column="detection_model_id",
            status_column="training_status__status",
            started_at_column="training_status__started_at",
            finished_at_column="training_status__finished_at",
            artifact_columns={
                "manifest": "training_status__manifest_path",
                "run_dir": "training_status__run_dir",
            },
            extra_columns=[
                OpsColumn("run_key", "Run ID", "training_status__run_key", link_to="training_run"),
                OpsColumn("frozen_dataset", "Frozen dataset", "detection_frozen_dataset_id", link_to="frozen_dataset"),
            ],
        ),
        relations=[
            OpsRelationSpec(
                id="model_trained_on_frozen_dataset",
                table="detection_model_is_trained_on_detection_frozen_dataset",
                from_entity="model",
                from_column="detection_model_id",
                to_entity="frozen_dataset",
                to_column="detection_frozen_dataset_id",
            )
        ],
        metrics=[
            OpsMetricTableSpec(
                id="model_metrics",
                title="Model metrics",
                table="pipeline_model__metrics_on_subset",
                metric_source="pipeline_model__metrics_on_subset",
                primary_key_columns=["detection_model_id", "subset_id"],
                entity_links={
                    "model": "detection_model_id",
                    "subset": "subset_id",
                },
                primary_columns=[
                    OpsColumn("model", "Model", "detection_model_id", filterable=True, link_to="model"),
                    OpsColumn("subset", "Subset", "subset_id", kind="chip", filterable=True),
                ],
                metric_columns=[
                    OpsColumnGroup(
                        "Precision",
                        [
                            OpsColumn("weighted_precision", "W-Precision", "calc__weighted_precision", kind="number"),
                            OpsColumn("macro_precision", "M-Precision", "calc__macro_precision", kind="number"),
                        ],
                    ),
                    OpsColumnGroup(
                        "Recall",
                        [
                            OpsColumn("weighted_recall", "W-Recall", "calc__weighted_recall", kind="number"),
                            OpsColumn("macro_recall", "M-Recall", "calc__macro_recall", kind="number"),
                        ],
                    ),
                    OpsColumnGroup(
                        "F1",
                        [
                            OpsColumn("weighted_f1", "W-F1", "calc__weighted_f1_score", kind="number"),
                            OpsColumn("macro_f1", "M-F1", "calc__macro_f1_score", kind="number"),
                        ],
                    ),
                    OpsColumn("accuracy", "Accuracy", "calc__accuracy", kind="number"),
                    OpsColumn("support", "Support", "calc__support", kind="number"),
                ],
                best_metric_column="calc__weighted_f1_score",
                default_sort=[("weighted_f1", "desc")],
                filters=[OpsColumn("subset_filter", "Subset", "subset_id", kind="chip", filterable=True)],
                default_filters=[OpsFilterRule(column_id="subset_id", operator="equal", value="val")],
            )
        ],
        class_metrics=[
            OpsClassMetricTableSpec(
                id="subset_class_metrics",
                title="Subset class metrics",
                table="pipeline_model__metrics_by_cls_on_subset",
                metric_source="pipeline_model__metrics_by_cls_on_subset",
                primary_key_columns=["detection_model_id", "subset_id", "label"],
                entity_links={
                    "model": "detection_model_id",
                    "subset": "subset_id",
                    "class": "label",
                },
                primary_columns=[
                    OpsColumn("model", "Model", "detection_model_id", filterable=True, link_to="model"),
                    OpsColumn("subset", "Subset", "subset_id", kind="chip", filterable=True),
                    OpsColumn("label", "Class", "label", filterable=True),
                ],
                metric_columns=[
                    OpsColumn("precision", "Precision", "calc__precision", kind="number"),
                    OpsColumn("recall", "Recall", "calc__recall", kind="number"),
                    OpsColumn("f1", "F1", "calc__f1_score", kind="number"),
                    OpsColumn("support", "Support", "calc__support", kind="number"),
                    OpsColumn("tp", "TP", "calc__TP", kind="number"),
                    OpsColumn("fp", "FP", "calc__FP", kind="number"),
                    OpsColumn("fn", "FN", "calc__FN", kind="number"),
                ],
                best_metric_column="calc__f1_score",
                default_sort=[("f1", "desc")],
                filters=[
                    OpsColumn("subset_filter", "Subset", "subset_id", kind="chip", filterable=True),
                    OpsColumn("class_filter", "Class", "label", filterable=True),
                ],
                default_filters=[OpsFilterRule(column_id="subset_id", operator="equal", value="val")],
            )
        ],
        tags=["yolo", "image", "training"],
    )
])
