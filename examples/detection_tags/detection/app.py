from __future__ import annotations

from datapipe.compute import Pipeline
from datapipe.datatable import DataStore
from datapipe.step.batch_transform import BatchTransform
from datapipe.types import Required
from datapipe_app import (
    DatapipeApp,
    DatapipeOpsSpec,
    OpsClassMetricTableSpec,
    OpsColumn,
    OpsColumnGroup,
    OpsDataSpec,
    OpsFilterRule,
    OpsImageAnnotationSpec,
    OpsImageDataSpec,
    OpsImagePredictionViewSpec,
    OpsImageRecordViewSpec,
    OpsFrozenDatasetSpec,
    OpsMetricTableSpec,
    OpsModelSpec,
    OpsRelationSpec,
    OpsTrainingSpec,
)
from datapipe_ml.metrics.model_selection import FindBestModel
from datapipe_ml.tasks.detection.freeze import DetectionFreezeDataset
from datapipe_ml.tasks.detection.inference import Inference_DetectionModel
from datapipe_ml.tasks.detection.train.yolov8 import Train_YoloV8_DetectionModel, YoloV8_TrainingConfig
from datapipe_ml.training.specs import TrainingResumeConfig, TrainingSyncConfig
from datapipe_ml.workflows.detection_classification.metrics import CountMetrics_Subset_PipelineModel

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
            outputs=["s3_images", "image__ground_truth", "tag", "image__tag", "image__subset_hint"],
            transform_keys=["request_id"],
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
            min_within_time="5min",
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
            labels=[("stage", "train"), ("stage", "count-metrics")],
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
            labels=[("stage", "train"), ("stage", "count-metrics")],
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
            labels=[("stage", "train"), ("stage", "count-metrics"), ("stage", "tag-metrics")],
        ),
        # --- FiftyOne (stage=fiftyone): GT + baseline/retrained predictions, filter by tag_id ---
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
            inputs=[
                "local_images",
                Required("image__ground_truth"),
                Required("image__subset"),
                Required("image__tag"),
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
)

ds = DataStore(DBCONN)
app = DatapipeApp(ds, catalog, pipeline)

app.add_specs([
    DatapipeOpsSpec(
        id="cat_dog",
        title="Cat/Dog Detection",
        description="YOLO training pipeline over frozen image snapshots.",
        icon="shield",
        color="blue",
        data=OpsDataSpec(
            tables=[
                "s3_images",
                "image__ground_truth",
                "image__subset",
                "tag",
                "image__tag",
                "detection_frozen_dataset",
                "detection_model_train",
                "pipeline_model__metrics_on_subset",
                "pipeline_model__metrics_by_cls_on_subset",
                "pipeline_model__metrics_by_tag_on_subset",
                "pipeline_model__metrics_by_tag_by_cls_on_subset",
            ],
            item_table="s3_images",
            label_table="image__ground_truth",
            subset_table="image__subset",
            image_view=OpsImageDataSpec(
                kind="image",
                image_table="s3_images",
                image_primary_key_columns=("image_name",),
                image_url_column="image_url",
                subset_table="image__subset",
                subset_join_columns={"image_name": "image_name"},
                subset_column="subset_id",
                ground_truth=OpsImageAnnotationSpec(
                    table="image__ground_truth",
                    primary_key_columns=("image_name",),
                    bboxes_column="bboxes",
                    labels_column="labels",
                    join_columns={"image_name": "image_name"},
                    role="gt",
                ),
                preview_size=84,
                modal_max_side=1100,
                detail_max_side=1600,
            ),
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
            record_view=OpsImageRecordViewSpec(
                kind="image",
                table="detection_frozen_dataset__has__image_gt",
                scope_column="detection_frozen_dataset_id",
                primary_key_columns=("image_name", "subset_id"),
                image_url_column="image__image_path",
                bboxes_column="bboxes",
                labels_column="labels",
                preview_size=84,
                modal_max_side=1100,
                detail_max_side=1600,
            ),
            columns=[
                OpsColumn(
                    "detection_frozen_dataset_id",
                    "detection_frozen_dataset_id",
                    "detection_frozen_dataset_id",
                    kind="link",
                    link_to="frozen_dataset",
                    filterable=True,
                ),
                OpsColumn(
                    "created_at",
                    "Frozen at",
                    "detection_frozen_dataset__created_at",
                    kind="datetime",
                    filterable=True,
                ),
                OpsColumn("split", "Split", "split", kind="split"),
                OpsColumn("models", "Models", "models_count", kind="models_count"),
            ],
            default_sort=[("created_at", "desc")],
        ),
        model=OpsModelSpec(
            table="detection_model_train",
            id_column="detection_model_id",
            artifact_uri_column="detection_model__model_path",
            is_best_table="attr__detection_model__is_best",
            is_best_column="detection_model__is_best",
            prediction_view=OpsImagePredictionViewSpec(
                kind="image",
                table="detection_prediction_train",
                model_id_column="detection_model_id",
                image_primary_key_columns=("image_name",),
                image_url_table="s3_images",
                image_url_column="image_url",
                image_url_join_columns={"image_name": "image_name"},
                prediction=OpsImageAnnotationSpec(
                    table="detection_prediction_train",
                    primary_key_columns=("image_name", "detection_model_id"),
                    bboxes_column="bboxes",
                    labels_column="labels",
                    scores_column="prediction__detection_scores",
                    role="prediction",
                ),
                ground_truth=OpsImageAnnotationSpec(
                    table="image__ground_truth",
                    primary_key_columns=("image_name",),
                    bboxes_column="bboxes",
                    labels_column="labels",
                    join_columns={"image_name": "image_name"},
                    role="gt",
                ),
                subset_table="image__subset",
                subset_join_columns={"image_name": "image_name"},
                subset_column="subset_id",
                metrics_on_image=OpsMetricTableSpec(
                    id="prediction_image_metrics",
                    title="Per-image metrics",
                    table="pipeline_model__metrics_on_image",
                    metric_source="pipeline_model__metrics_on_image",
                    primary_key_columns=["image_name", "detection_model_id", "subset_id", "label"],
                    entity_links={},
                    primary_columns=[],
                    metric_columns=[
                        OpsColumn("subset_id", "subset_id", "subset_id", kind="chip", filterable=True),
                        OpsColumn("tp", "TP", "calc__TP", kind="number"),
                        OpsColumn("fp", "FP", "calc__FP", kind="number"),
                        OpsColumn("fn", "FN", "calc__FN", kind="number"),
                        OpsColumn("fp_extra", "FP (extra bbox)", "calc__FP_extra_bbox", kind="number"),
                        OpsColumn("tp_extra", "TP (extra bbox)", "calc__TP_extra_bbox", kind="number"),
                    ],
                ),
                preview_size=84,
                modal_max_side=1100,
                detail_max_side=1600,
            ),
        ),
        training=OpsTrainingSpec(
            status_table="detection_training_status",
            artifact_columns={
                "manifest": "training_status__manifest_path",
                "run_dir": "training_status__run_dir",
            },
            columns=[
                OpsColumn(
                    "run_id",
                    "Run ID",
                    "training_status__run_key",
                    kind="link",
                    link_to="training_run",
                    filterable=True,
                ),
                OpsColumn(
                    "detection_model_id",
                    "detection_model_id",
                    "detection_model_id",
                    kind="link",
                    link_to="model",
                    filterable=True,
                ),
                OpsColumn(
                    "detection_frozen_dataset_id",
                    "detection_frozen_dataset_id",
                    "detection_frozen_dataset_id",
                    kind="link",
                    link_to="frozen_dataset",
                    filterable=True,
                ),
                OpsColumn(
                    "started_at",
                    "Started",
                    "training_status__started_at",
                    kind="datetime",
                    filterable=True,
                ),
                OpsColumn("duration", "Duration", "duration_seconds", kind="duration"),
                OpsColumn(
                    "status",
                    "Status",
                    "training_status__status",
                    kind="status",
                    filterable=True,
                ),
            ],
            default_sort=[("started_at", "desc")],
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
                    OpsColumn(
                        "detection_model_id",
                        "detection_model_id",
                        "detection_model_id",
                        filterable=True,
                        link_to="model",
                    ),
                    OpsColumn("subset_id", "subset_id", "subset_id", kind="chip", filterable=True),
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
                filters=[OpsColumn("subset_filter", "subset_id", "subset_id", kind="chip", filterable=True)],
                default_filters=[OpsFilterRule(column_id="subset_id", operator="equal", value="val")],
            ),
            OpsMetricTableSpec(
                id="tag_metrics_on_subset",
                title="Tag metrics on subset",
                table="pipeline_model__metrics_by_tag_on_subset",
                metric_source="pipeline_model__metrics_by_tag_on_subset",
                primary_key_columns=["detection_model_id", "tag_id", "subset_id"],
                entity_links={
                    "model": "detection_model_id",
                    "subset": "subset_id",
                    "tag": "tag_id",
                },
                primary_columns=[
                    OpsColumn(
                        "detection_model_id",
                        "detection_model_id",
                        "detection_model_id",
                        filterable=True,
                        link_to="model",
                    ),
                    OpsColumn("tag_id", "tag_id", "tag_id", filterable=True),
                    OpsColumn("subset_id", "subset_id", "subset_id", kind="chip", filterable=True),
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
                filters=[
                    OpsColumn("subset_filter", "subset_id", "subset_id", kind="chip", filterable=True),
                    OpsColumn("tag_filter", "tag_id", "tag_id", filterable=True),
                ],
                default_filters=[OpsFilterRule(column_id="subset_id", operator="equal", value="val")],
            ),
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
                    OpsColumn(
                        "detection_model_id",
                        "detection_model_id",
                        "detection_model_id",
                        filterable=True,
                        link_to="model",
                    ),
                    OpsColumn("subset_id", "subset_id", "subset_id", kind="chip", filterable=True),
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
                    OpsColumn("subset_filter", "subset_id", "subset_id", kind="chip", filterable=True),
                    OpsColumn("class_filter", "Class", "label", filterable=True),
                ],
                default_filters=[OpsFilterRule(column_id="subset_id", operator="equal", value="val")],
            ),
            OpsClassMetricTableSpec(
                id="tag_class_metrics_on_subset",
                title="Tag class metrics on subset",
                table="pipeline_model__metrics_by_tag_by_cls_on_subset",
                metric_source="pipeline_model__metrics_by_tag_by_cls_on_subset",
                primary_key_columns=["detection_model_id", "tag_id", "subset_id", "label"],
                entity_links={
                    "model": "detection_model_id",
                    "subset": "subset_id",
                    "tag": "tag_id",
                    "class": "label",
                },
                primary_columns=[
                    OpsColumn(
                        "detection_model_id",
                        "detection_model_id",
                        "detection_model_id",
                        filterable=True,
                        link_to="model",
                    ),
                    OpsColumn("tag_id", "tag_id", "tag_id", filterable=True),
                    OpsColumn("subset_id", "subset_id", "subset_id", kind="chip", filterable=True),
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
                    OpsColumn("subset_filter", "subset_id", "subset_id", kind="chip", filterable=True),
                    OpsColumn("tag_filter", "tag_id", "tag_id", filterable=True),
                    OpsColumn("class_filter", "Class", "label", filterable=True),
                ],
                default_filters=[OpsFilterRule(column_id="subset_id", operator="equal", value="val")],
            ),
        ],
        tags=["yolo", "image", "tags", "training"],
    ),
])
