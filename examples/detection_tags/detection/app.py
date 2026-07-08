from __future__ import annotations

from datapipe.compute import Pipeline
from datapipe.datatable import DataStore
from datapipe.step.batch_transform import BatchTransform
from datapipe_app import (
    DatapipeApp,
    DatapipeOpsSpec,
    OpsColumn,
    OpsDataSpec,
    OpsFrozenDatasetSpec,
    OpsMetricTableSpec,
    OpsModelSpec,
    OpsRelationSpec,
    OpsTrainingSpec,
)
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
                YoloV8_TrainingConfig(model="yolov8n.pt", imgsz=320, batch=10, epochs=10, exist_ok=True)
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

app.add_specs([
    DatapipeOpsSpec(
        id="detection_tags_yolo",
        title="Detection Tags YOLO",
        description=(
            "YOLO detection with tagged scenario batches. Compare overall subset metrics and "
            "per-tag precision/recall after retraining on tagged data."
        ),
        icon="tags",
        color="purple",
        data=OpsDataSpec(
            tables=[
                "s3_images",
                "image__ground_truth",
                "image__subset",
                "tag",
                "image__tag",
                "detection_frozen_dataset",
                "detection_model_train",
                "detection_model_train__metrics_on_subset",
                "tag_metrics",
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
            table="detection_model_train",
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
                table="detection_model_train__metrics_on_subset",
                metric_source="detection_model_train__metrics_on_subset",
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
                    OpsColumn("images_support", "Images", "calc__images_support", kind="number"),
                    OpsColumn("support", "Support", "calc__support", kind="number"),
                    OpsColumn("tp", "TP", "calc__TP", kind="number"),
                    OpsColumn("fp", "FP", "calc__FP", kind="number"),
                    OpsColumn("fn", "FN", "calc__FN", kind="number"),
                    OpsColumn("iou_mean", "IoU mean", "calc__iou_mean", kind="number"),
                    OpsColumn("accuracy", "Accuracy", "calc__accuracy", kind="number"),
                    OpsColumn("precision", "Precision", "calc__precision", kind="number"),
                    OpsColumn("recall", "Recall", "calc__recall", kind="number"),
                    OpsColumn("f1", "F1", "calc__f1_score", kind="number"),
                ],
                best_metric_column="calc__f1_score",
                default_sort=[("f1", "desc")],
                filters=[OpsColumn("subset_filter", "Subset", "subset_id", kind="chip", filterable=True)],
            ),
            OpsMetricTableSpec(
                id="tag_metrics",
                title="Tag metrics",
                table="tag_metrics",
                metric_source="tag_metrics",
                primary_key_columns=["detection_model_id", "tag_id", "subset_id"],
                entity_links={
                    "model": "detection_model_id",
                    "subset": "subset_id",
                    "tag": "tag_id",
                },
                primary_columns=[
                    OpsColumn("model", "Model", "detection_model_id", filterable=True, link_to="model"),
                    OpsColumn("tag", "Tag", "tag_id", filterable=True),
                    OpsColumn("subset", "Subset", "subset_id", kind="chip", filterable=True),
                ],
                metric_columns=[
                    OpsColumn("images_support", "Images", "calc__images_support", kind="number"),
                    OpsColumn("support", "Support", "calc__support", kind="number"),
                    OpsColumn("tp", "TP", "calc__TP", kind="number"),
                    OpsColumn("fp", "FP", "calc__FP", kind="number"),
                    OpsColumn("fn", "FN", "calc__FN", kind="number"),
                    OpsColumn("precision", "Precision", "calc__precision", kind="number"),
                    OpsColumn("recall", "Recall", "calc__recall", kind="number"),
                    OpsColumn("f1", "F1", "calc__f1_score", kind="number"),
                ],
                best_metric_column="calc__f1_score",
                default_sort=[("f1", "desc")],
                filters=[
                    OpsColumn("subset_filter", "Subset", "subset_id", kind="chip", filterable=True),
                    OpsColumn("tag_filter", "Tag", "tag_id", filterable=True),
                ],
            ),
        ],
        class_metrics=[],
        tags=["yolo", "image", "tags", "training"],
    )
])
