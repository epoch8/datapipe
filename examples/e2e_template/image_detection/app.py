from __future__ import annotations

from datapipe.compute import DatapipeApp, Pipeline, PipelineStep, ComputeStep
from datapipe.datatable import DataStore
from datapipe.step.batch_generate import BatchGenerate
from datapipe.step.batch_transform import BatchTransform
from datapipe_label_studio.types import S3Bucket
from datapipe_label_studio.upload_predictions_pipeline import LabelStudioUploadPredictions
from datapipe_label_studio.upload_tasks_pipeline import LabelStudioUploadTasks
from datapipe_ml.metrics.model_selection import FindBestModel
from datapipe_ml.tasks.detection.freeze import DetectionFreezeDataset
from datapipe_ml.tasks.detection.inference import Inference_DetectionModel
from datapipe_ml.tasks.detection.metrics import CountMetrics_Subset_DetectionModel

from examples.e2e_template.common import ServiceSettings
from examples.e2e_template.image_detection import steps
from examples.e2e_template.image_detection.config import LABEL_CONFIG, PROJECT_NAME
from examples.e2e_template.image_detection.data import build_catalog


class PlaceholderTrainingStep(PipelineStep):
    labels = [("stage", "train")]

    def build_compute(self, ds, catalog) -> list[ComputeStep]:
        return []


def _training_step(settings: ServiceSettings, *, include_training: bool) -> PipelineStep:
    if not include_training:
        return PlaceholderTrainingStep()
    from datapipe_ml.tasks.detection.train.yolov8 import Train_YoloV8_DetectionModel, YoloV8_TrainingConfig

    return Train_YoloV8_DetectionModel(  # type: ignore[return-value]
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
        output__detection_frozen_dataset__class_names="detection_frozen_dataset__class_names",
        max_within_time="1w",
        working_dir=str(settings.datapipe_dir),
        tmp_folder=str(settings.datapipe_dir / "tmp"),
        yolov8_train_configs=[
            YoloV8_TrainingConfig(
                model="yolo11n.pt",
                imgsz=16,
                batch=2,
                epochs=1,
                seed=42,
                device="cpu",
                workers=0,
                patience=1,
                amp=False,
                val=False,
                plots=False,
            )
        ],
        primary_keys=["image_name"],
        bbox_id__name=None,
        labels=[("stage", "train")],
        create_table=True,
        ignore_errors_sample_sizes=True,
        model_suffix="_e2e",
    )


def build_pipeline(settings: ServiceSettings, *, include_training: bool = True) -> Pipeline:
    return Pipeline(
        [
            BatchGenerate(
                lambda: steps.list_s3_images(settings),
                outputs=["s3_images"],
                labels=[("stage", "annotation")],
            ),
            BatchGenerate(
                steps.list_detection_models,
                outputs=["detection_model"],
                labels=[("stage", "annotation")],
            ),
            BatchTransform(
                func=steps.untracked_inference,
                inputs=["s3_images"],
                outputs=["detection_predictions"],
                transform_keys=["image_name"],
                kwargs=dict(
                    best_model_table="best_detection_model",
                    trained_models_table="detection_model_train",
                    fallback_model_table="detection_model",
                    model_id_column="detection_model_id",
                    primary_keys=["image_name"],
                    image__image_path__name="image_url",
                    bbox_id__name="bbox_id",
                    batch_size_default=1,
                    detection_model_primary_keys=["detection_model_id"],
                    modules_to_hide_when_loading_detection_model=[],
                ),
                labels=[("stage", "annotation")],
            ),
            BatchTransform(
                func=steps.bboxes_to_ls_prediction,
                inputs=["detection_predictions", "s3_images"],
                outputs=["images_with_predictions"],
                transform_keys=["image_name"],
                labels=[("stage", "annotation")],
                kwargs=dict(image__image_path__name="image_url"),
            ),
            LabelStudioUploadTasks(
                input__item="s3_images",
                output__label_studio_project_task="ls_task",
                output__label_studio_project_annotation="ls_annotations",
                output__label_studio_sync_table="ls_sync",
                ls_url=settings.label_studio_url,
                api_key=settings.label_studio_api_key,
                project_identifier=PROJECT_NAME,
                project_label_config_at_create=LABEL_CONFIG,
                primary_keys=["image_name"],
                columns=["image_url"],
                storages=[
                    S3Bucket(
                        bucket=settings.s3_bucket,
                        key=settings.aws_key,
                        secret=settings.aws_secret,
                        region_name=settings.aws_region,
                        endpoint_url=settings.s3_endpoint_url,
                    )
                ],
                chunk_size=100,
                labels=[("stage", "annotation"), ("stage", "ls-sync")],
            ),
            LabelStudioUploadPredictions(
                input__item__has__prediction="images_with_predictions",
                input__label_studio_project_task="ls_task",
                output__label_studio_project_prediction="ls_predictions",
                ls_url=settings.label_studio_url,
                api_key=settings.label_studio_api_key,
                project_identifier=PROJECT_NAME,
                primary_keys=["image_name"],
                model_version__column="detection_model_id",
                labels=[("stage", "annotation"), ("stage", "ls-sync")],
            ),
            BatchTransform(
                func=steps.parse_annotations_from_label_studio,
                inputs=["ls_annotations"],
                outputs=["image__ground_truth"],
                labels=[("stage", "annotation"), ("stage", "ls-sync")],
                transform_keys=["image_name"],
            ),
            BatchTransform(
                func=steps.split_df_train_val,
                inputs=["image__ground_truth"],
                outputs=["image__subset"],
                labels=[("stage", "train"), ("stage", "train-prepare")],
                transform_keys=["image_name"],
            ),
            DetectionFreezeDataset(  # type: ignore[list-item]
                input__image="s3_images",
                input__image__ground_truth="image__ground_truth",
                input__subset__has__image="image__subset",
                output__detection_frozen_dataset="detection_frozen_dataset",
                output__detection_frozen_dataset__has__image_gt="detection_frozen_dataset__has__image_gt",
                working_dir=str(settings.datapipe_dir),
                min_within_time="0s",
                min_delta=1,
                primary_keys=["image_name"],
                bbox_id__name=None,
                image__image_path__name="image_url",
                labels=[("stage", "train"), ("stage", "train-prepare")],
                create_table=True,
            ),
            _training_step(settings, include_training=include_training),
            Inference_DetectionModel(
                input__image="s3_images",
                input__detection_model="detection_model_train",
                output__detection_prediction="detection_prediction_train",
                primary_keys=["image_name"],
                bbox_id__name=None,
                labels=[("stage", "train"), ("stage", "inference")],
                image__image_path__name="image_url",
                batch_size_default=1,
                create_table=True,
                modules_to_hide_when_loading_detection_model=[],
            ),
            CountMetrics_Subset_DetectionModel(
                input__image__ground_truth="image__ground_truth",
                input__subset__has__image="image__subset",
                input__detection_prediction="detection_prediction_train",
                output__detection_model__metrics__on__image="detection_model_train__metrics_on_image",
                output__detection_model__metrics__on__subset="detection_model_train__metrics_on_subset",
                primary_keys=["image_name"],
                bbox_id__name=None,
                labels=[("stage", "train"), ("stage", "count-metrics")],
                minimum_iou=0.5,
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
            BatchTransform(
                func=steps.download_images,
                inputs=["s3_images"],
                outputs=["local_images"],
                transform_keys=["image_name"],
                labels=[("stage", "fiftyone")],
                kwargs=dict(
                    settings=settings,
                    image__image_path__name="image_url",
                    image__local_image_path__name="local_path",
                ),
            ),
            BatchTransform(
                func=steps.publish_to_fiftyone,
                inputs=["local_images", "detection_prediction_train"],
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


def build_app(
    settings: ServiceSettings | None = None,
    *,
    include_training: bool = True,
    include_fiftyone: bool = True,
) -> DatapipeApp:
    settings = settings or ServiceSettings.from_env()
    return DatapipeApp(
        DataStore(settings.dbconn, create_meta_table=True),
        build_catalog(settings, include_fiftyone=include_fiftyone),
        build_pipeline(settings, include_training=include_training),
    )


app = None

