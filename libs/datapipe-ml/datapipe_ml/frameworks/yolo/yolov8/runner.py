import json
import logging
import multiprocessing as mp
import os
import shutil
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, List, Literal, Optional, Union

import numpy as np
import ultralytics
from PIL import Image

from datapipe_ml.core.multiprocessing import finish_training_subprocess
from datapipe_ml.core.training_subprocess import run_training_subprocess_body
from datapipe_ml.training.paths import default_tmp_folder, default_train_project_dir
from datapipe_ml.frameworks.yolo.artifacts import (
    YoloDataYAMLConfig,
    yolo_best_threshold_from_ultralytics_metrics,
    yolo_collect_results_generic,
    yolo_finalize_training_output,
    yolo_count_objects,
    yolo_load_data_config,
    yolo_prepare_tmp_dirs_for_cloud_yolov8,
    yolo_select_last_exp,
)
from datapipe_ml.frameworks.yolo.train_session import (
    YoloTrainSession,
    yolo_write_exp_metadata,
)
from datapipe_ml.training.specs import TrainingSyncConfig

logger = logging.getLogger("datapipe.ml.yolov8.script")


@dataclass
class YoloV8_TrainingConfig:
    # datapipe-ml arguments:
    tmp_folder: str = field(default_factory=default_tmp_folder)  # When used cloud images, store them to this folder
    initial_weights_path: Optional[str] = None
    persisted_project_dir: Optional[str] = None

    # yolov8 arguments
    model: str = "yolov8l-seg.pt"  # Specifies the model file for training
    data: Optional[Union[str, YoloDataYAMLConfig]] = "coco128.yaml"  # YoloV8_Data_YAMLConfig or dataset.yaml path
    epochs: int = 300  # Total number of training epochs
    time: Optional[int] = None  # Maximum training time in hours.
    patience: int = 100  # EarlyStopping patience (epochs without improvement)
    batch: int = 16  # Batch size for training
    imgsz: int = 640  # Defines the image size for inference
    save: bool = True  # Enables saving of training checkpoints and final model weights.
    save_period: int = -1  # Save checkpoint every x epochs (disabled if < 1)
    cache: Optional[bool] = (
        False  # Enables caching of dataset images in memory (True/ram), on disk (disk), or disables it (False)
    )
    device: Optional[str] = None  # Specifies the computational device(s) for training: a single GPU (device=0)
    workers: int = 8  # Number of worker threads for data loading (per RANK if Multi-GPU training)
    project: Optional[str] = field(default_factory=default_train_project_dir)
    name: str = (
        "exp"  # Name of the training run. Used for creating a subdirectory within the project folder, where training logs and outputs are stored.
    )
    exist_ok: bool = False  # If True, allows overwriting of an existing project/name directory.
    pretrained: bool = True  # Determines whether to start training from a pretrained model.
    optimizer: str = (
        "auto"  # Choice of optimizer for training. Options include SGD, Adam, AdamW, NAdam, RAdam, RMSProp etc.
    )
    verbose: bool = False  # Enables verbose output during training, providing detailed logs and progress updates.
    seed: int = 0  # Sets the random seed for training
    deterministic: bool = (
        True  # Forces deterministic algorithm use, ensuring reproducibility but may affect performance and speed
    )
    single_cls: bool = False  # Treats all classes in multi-class datasets as a single class during training.
    rect: bool = False  # Enables rectangular training, optimizing batch composition for minimal padding
    cos_lr: bool = (
        False  # Utilizes a cosine learning rate scheduler, adjusting the learning rate following a cosine curve over epochs
    )
    close_mosaic: int = (
        10  # Disables mosaic data augmentation in the last N epochs to stabilize training before completion.
    )
    resume: bool = False  # Resumes training from the last saved checkpoint.
    amp: bool = True  # Enables Automatic Mixed Precision (AMP) training
    fraction: float = 1.0  # Specifies the fraction of the dataset to use for training.
    profile: bool = False  # Enables profiling of ONNX and TensorRT speeds during training
    freeze: Optional[int] = (
        None  # Freezes the first N layers of the model or specified layers by index, reducing the number of trainable parameters
    )
    lr0: float = 0.01  # Initial learning rate (i.e. SGD=1E-2, Adam=1E-3)
    lrf: float = 0.01  # Final learning rate as a fraction of the initial rate = (lr0 * lrf)
    momentum: float = 0.937  # Momentum factor for SGD or beta1 for Adam optimizers
    weight_decay: float = 0.0005  # L2 regularization term, penalizing large weights to prevent overfitting.
    warmup_epochs: float = 3.0  # Number of epochs for learning rate warmup
    warmup_momentum: float = 0.8  # Initial momentum for warmup phase
    warmup_bias_lr: float = 0.1  # Learning rate for bias parameters during the warmup phase
    box: float = 7.5  # Weight of the box loss component in the loss function
    cls: float = 0.5  # Weight of the classification loss in the total loss function
    dfl: float = 1.5  # Weight of the distribution focal loss
    pose: float = 12.0  # Weight of the pose loss in models trained for pose estimation,
    kobj: float = 2.0  # Weight of the keypoint objectness loss in pose estimation models
    label_smoothing: float = 0.0  # Applies label smoothing, softening hard labels to a mix of the target label
    nbs: int = 64  # Nominal batch size for normalization of loss.
    overlap_mask: bool = True  # Determines whether segmentation masks should overlap during training
    mask_ratio: float = (
        4  # Downsample ratio for segmentation masks, affecting the resolution of masks used during training.
    )
    dropout: float = 0.0  # Dropout rate for regularization in classification tasks
    val: bool = True  # Enables validation during training
    plots: bool = True  # Generates and saves plots of training and validation metrics

    # Data augmentation (Ultralytics train args; defaults disable augmentations — see docs.ultralytics.com/guides/yolo-data-augmentation)
    multi_scale: bool = False  # Vary imgsz each batch by +/- 50% (YOLOv5-style; Ultralytics also accepts float 0.0–1.0)
    degrees: float = 0.0  # Random rotation (+/- deg), range 0.0–180
    translate: float = 0.0  # Random translation (+/- fraction of image size), range 0.0–1.0
    scale: float = 0.0  # Random scale (+/- gain), range 0.0–1.0
    shear: float = 0.0  # Random shear (+/- deg)
    perspective: float = 0.0  # Random perspective (+/- fraction)
    fliplr: float = 0.0  # Horizontal flip probability, range 0.0–1.0
    flipud: float = 0.0  # Vertical flip probability, range 0.0–1.0
    mosaic: float = 0.0  # Mosaic augmentation probability (combines 4 images), range 0.0–1.0
    mixup: float = 0.0  # MixUp augmentation probability, range 0.0–1.0
    copy_paste: float = 0.0  # Copy-paste augmentation probability (segmentation), range 0.0–1.0
    copy_paste_mode: Optional[str] = None  # Copy-paste strategy: 'flip' or 'mixup' (segmentation)
    hsv_h: float = 0.0  # HSV-Hue augmentation (fraction)
    hsv_s: float = 0.0  # HSV-Saturation augmentation (fraction)
    hsv_v: float = 0.0  # HSV-Value augmentation (fraction)
    bgr: float = 0.0  # BGR channel swap probability
    cutmix: float = 0.0  # CutMix augmentation probability, range 0.0–1.0
    erasing: float = 0.0  # Random erasing probability (classification-style regularization)
    auto_augment: Optional[str] = None  # Auto augmentation policy, e.g. 'randaugment'; None disables

    def to_yolo_kwargs(self):
        training_yolo_arguments = [
            "model",
            "data",
            "epochs",
            "time",
            "patience",
            "batch",
            "imgsz",
            "save",
            "save_period",
            "cache",
            "device",
            "workers",
            "project",
            "name",
            "exist_ok",
            "pretrained",
            "optimizer",
            "verbose",
            "seed",
            "deterministic",
            "single_cls",
            "rect",
            "cos_lr",
            "close_mosaic",
            "resume",
            "amp",
            "fraction",
            "profile",
            "freeze",
            "lr0",
            "lrf",
            "momentum",
            "weight_decay",
            "warmup_epochs",
            "warmup_momentum",
            "warmup_bias_lr",
            "box",
            "cls",
            "dfl",
            "pose",
            "kobj",
            "label_smoothing",
            "nbs",
            "overlap_mask",
            "mask_ratio",
            "dropout",
            "val",
            "plots",
            "multi_scale",
            "degrees",
            "translate",
            "scale",
            "shear",
            "perspective",
            "fliplr",
            "flipud",
            "mosaic",
            "mixup",
            "copy_paste",
            "copy_paste_mode",
            "hsv_h",
            "hsv_s",
            "hsv_v",
            "bgr",
            "cutmix",
            "erasing",
            "auto_augment",
        ]

        full_dict = asdict(self)

        kwargs = {k: full_dict[k] for k in training_yolo_arguments if k in full_dict}
        if kwargs.get("copy_paste_mode") is None:
            kwargs.pop("copy_paste_mode", None)
        return kwargs


@dataclass
class TrainingResult:
    detection_model_id: str
    class_names: List[str]
    epoch: int
    model_path: Optional[str]
    train_box_loss: float
    train_cls_loss: float
    train_dfl_loss: float
    metrics_precision: float
    metrics_recall: float
    metrics_mAP_0_5: float
    metrics_mAP_0_5_to_0_95: float
    val_box_loss: float
    val_cls_loss: float
    val_dfl_loss: float
    lr_pg0: float  # https://github.com/ultralytics/yolov8/issues/839#issuecomment-919821124
    lr_pg1: float
    lr_pg2: float
    objects_count: Optional[int]
    f1_curve_image: Optional[np.ndarray]
    best_threshold: Optional[float]


@dataclass
class TrainingSegmentationResult:
    segmentation_model_id: str
    class_names: List[str]
    epoch: int
    model_path: Optional[str]
    train_box_loss: float
    train_cls_loss: float
    train_seg_loss: float
    train_dfl_loss: float
    metrics_precision_box: float
    metrics_recall_box: float
    metrics_mAP_0_5_box: float
    metrics_mAP_0_5_to_0_95_box: float
    metrics_precision_mask: float
    metrics_recall_mask: float
    metrics_mAP_0_5_mask: float
    metrics_mAP_0_5_to_0_95_mask: float
    val_box_loss: float
    val_cls_loss: float
    val_seg_loss: float
    val_dfl_loss: float
    x_pg0: float
    x_pg1: float
    x_pg2: float
    objects_count: Optional[int]
    f1_curve_image: Optional[np.ndarray]
    best_threshold: Optional[float]


@dataclass
class TrainingKeypointsResult:
    keypoints_model_id: str
    class_names: List[str]
    epoch: int
    model_path: Optional[str]
    train_box_loss: float
    train_pose_loss: float
    train_kobj_loss: float
    train_cls_loss: float
    train_dfl_loss: float
    metrics_precision_pose: float
    metrics_recall_pose: float
    metrics_mAP_0_5_pose: float
    metrics_mAP_0_5_to_0_95_pose: float
    val_box_loss: float
    val_pose_loss: float
    val_kobj_loss: float
    val_cls_loss: float
    val_dfl_loss: float
    x_pg0: float
    x_pg1: float
    x_pg2: float
    objects_count: Optional[int]
    f1_curve_image: Optional[np.ndarray]
    best_threshold: Optional[float]


@dataclass
class TrainModelResult:
    training_results: Optional[
        Union[List[TrainingResult], List[TrainingSegmentationResult], List[TrainingKeypointsResult]]
    ] = None
    traceback_logs: Optional[str] = None


def train_model(
    yolov8_training_config: YoloV8_TrainingConfig,
    objects_count: Optional[int],
    class_names_in: List[str],
    image_filepaths: List[str],
    coco_txt_filepaths: List[str],
    sync_config: Optional[TrainingSyncConfig],
    task: Literal["detect", "segment", "pose"],
) -> Optional[Union[List[TrainingSegmentationResult], List[TrainingResult], List[TrainingKeypointsResult]]]:

    project_dir = yolov8_training_config.project or default_train_project_dir()
    yolov8_training_config.project = project_dir
    if yolov8_training_config.data is None:
        yolov8_training_config.data = "coco128.yaml"

    if objects_count is None:
        data_cfg = (
            yolo_load_data_config(yolov8_training_config.data)
            if isinstance(yolov8_training_config.data, (str, Path))
            else yolov8_training_config.data
        )
        objects_count = yolo_count_objects(data_cfg)

    (
        src_images_dir_path,
        src_project_path,
        tmp_dir_images,
        tmp_dir_project,
        tmp_dir_images_cls,
        tmp_dir_project_cls,
        tmp_dir_model_cls,
    ) = yolo_prepare_tmp_dirs_for_cloud_yolov8(
        yolov8_training_config,
        image_filepaths=image_filepaths,
        coco_txt_filepaths=coco_txt_filepaths,
    )
    train_project_dir: str = yolov8_training_config.project or project_dir

    session = YoloTrainSession(
        training_config=yolov8_training_config,
        sync_config=sync_config,
        src_project_path=src_project_path,
        tmp_dir_project=tmp_dir_project,
    )

    def _launch(class_names: List[str], tmp_yaml_path: Optional[Path]) -> tuple[Optional[Path], Any, Any]:
        os.environ.setdefault("WANDB_DISABLED", "true")
        model = ultralytics.YOLO(yolov8_training_config.model)
        logger.info("Train %s model", yolov8_training_config.model)
        _ = model.train(**yolov8_training_config.to_yolo_kwargs())
        trainer = model.trainer
        validator = trainer.validator if trainer is not None else None
        metrics = validator.metrics if validator is not None else None
        selected_exp_folder = yolo_select_last_exp(train_project_dir, yolov8_training_config.name)
        if selected_exp_folder is not None:
            yolo_write_exp_metadata(selected_exp_folder, tmp_yaml_path, class_names)
        return selected_exp_folder, trainer, metrics

    (exp_folder, trainer, metrics), class_names = session.run(_launch)

    if exp_folder is None:
        return None

    # F1 curve images
    f1_curve_image = None
    f1_mask_curve_image = None
    best_threshold = 0.45
    if task == "detect":
        if (exp_folder / "F1_curve.png").exists():
            f1_curve_image = np.array(Image.open(exp_folder / "F1_curve.png"))
        best_threshold = yolo_best_threshold_from_ultralytics_metrics(metrics, "F1-Confidence(B)")
    elif task == "segment":
        if (exp_folder / "MaskF1_curve.png").exists():
            f1_mask_curve_image = np.array(Image.open(exp_folder / "MaskF1_curve.png"))
        best_threshold = yolo_best_threshold_from_ultralytics_metrics(metrics, "F1-Confidence(M)")
    else:  # pose
        if (exp_folder / "PoseF1_curve.png").exists():
            f1_curve_image = np.array(Image.open(exp_folder / "PoseF1_curve.png"))
        elif (exp_folder / "F1_curve.png").exists():
            f1_curve_image = np.array(Image.open(exp_folder / "F1_curve.png"))
        best_threshold = yolo_best_threshold_from_ultralytics_metrics(metrics, "F1-Confidence(P)")

    exp_folder = yolo_finalize_training_output(
        exp_folder,
        persisted_project_dir=yolov8_training_config.persisted_project_dir or src_project_path,
        tmp_dir_images_cls=tmp_dir_images_cls,
        tmp_dir_project_cls=tmp_dir_project_cls,
        tmp_dir_model_cls=tmp_dir_model_cls,
    )

    if task == "detect":
        rename_map = {
            "epoch": "epoch",
            "train/box_loss": "train_box_loss",
            "train/cls_loss": "train_cls_loss",
            "train/dfl_loss": "train_dfl_loss",
            "metrics/precision(B)": "metrics_precision",
            "metrics/recall(B)": "metrics_recall",
            "metrics/mAP50(B)": "metrics_mAP_0_5",
            "metrics/mAP50-95(B)": "metrics_mAP_0_5_to_0_95",
            "val/box_loss": "val_box_loss",
            "val/cls_loss": "val_cls_loss",
            "val/dfl_loss": "val_dfl_loss",
            "lr/pg0": "lr_pg0",
            "lr/pg1": "lr_pg1",
            "lr/pg2": "lr_pg2",
        }
        return yolo_collect_results_generic(
            exp_folder=str(exp_folder),
            result_cls=TrainingResult,
            id_field_name="detection_model_id",
            id_field_value=yolov8_training_config.name,
            class_names=class_names,
            objects_count=objects_count,
            f1_image_field_name="f1_curve_image",
            f1_image=f1_curve_image,
            best_threshold=best_threshold,
            rename_map=rename_map,
            best_metric_col="metrics_mAP_0_5_to_0_95",
            weights_subdir="weights",
        )
    if task == "segment":
        rename_map = {
            "epoch": "epoch",
            "train/box_loss": "train_box_loss",
            "train/cls_loss": "train_cls_loss",
            "train/seg_loss": "train_seg_loss",
            "train/dfl_loss": "train_dfl_loss",
            "metrics/precision(B)": "metrics_precision_box",
            "metrics/recall(B)": "metrics_recall_box",
            "metrics/mAP50(B)": "metrics_mAP_0_5_box",
            "metrics/mAP50-95(B)": "metrics_mAP_0_5_to_0_95_box",
            "metrics/precision(M)": "metrics_precision_mask",
            "metrics/recall(M)": "metrics_recall_mask",
            "metrics/mAP50(M)": "metrics_mAP_0_5_mask",
            "metrics/mAP50-95(M)": "metrics_mAP_0_5_to_0_95_mask",
            "val/box_loss": "val_box_loss",
            "val/cls_loss": "val_cls_loss",
            "val/seg_loss": "val_seg_loss",
            "val/dfl_loss": "val_dfl_loss",
            "lr/pg0": "x_pg0",
            "lr/pg1": "x_pg1",
            "lr/pg2": "x_pg2",
        }
        return yolo_collect_results_generic(
            exp_folder=str(exp_folder),
            result_cls=TrainingSegmentationResult,
            id_field_name="segmentation_model_id",
            id_field_value=yolov8_training_config.name,
            class_names=class_names,
            objects_count=objects_count,
            f1_image_field_name="f1_curve_image",
            f1_image=f1_mask_curve_image,
            best_threshold=best_threshold,
            rename_map=rename_map,
            best_metric_col="metrics_mAP_0_5_to_0_95_mask",
            weights_subdir="weights",
        )
    rename_map = {
        "epoch": "epoch",
        "train/box_loss": "train_box_loss",
        "train/pose_loss": "train_pose_loss",
        "train/kobj_loss": "train_kobj_loss",
        "train/cls_loss": "train_cls_loss",
        "train/dfl_loss": "train_dfl_loss",
        "metrics/precision(P)": "metrics_precision_pose",
        "metrics/recall(P)": "metrics_recall_pose",
        "metrics/mAP50(P)": "metrics_mAP_0_5_pose",
        "metrics/mAP50-95(P)": "metrics_mAP_0_5_to_0_95_pose",
        "val/box_loss": "val_box_loss",
        "val/pose_loss": "val_pose_loss",
        "val/kobj_loss": "val_kobj_loss",
        "val/cls_loss": "val_cls_loss",
        "val/dfl_loss": "val_dfl_loss",
        "lr/pg0": "x_pg0",
        "lr/pg1": "x_pg1",
        "lr/pg2": "x_pg2",
    }
    return yolo_collect_results_generic(
        exp_folder=str(exp_folder),
        result_cls=TrainingKeypointsResult,
        id_field_name="keypoints_model_id",
        id_field_value=yolov8_training_config.name,
        class_names=class_names,
        objects_count=objects_count,
        f1_image_field_name="f1_curve_image",
        f1_image=f1_curve_image,
        best_threshold=best_threshold,
        rename_map=rename_map,
        best_metric_col="metrics_mAP_0_5_to_0_95_pose",
        weights_subdir="weights",
    )


def train_process(
    queue: mp.Queue,
    yolov8_training_config: YoloV8_TrainingConfig,
    objects_count: int,
    class_names: List[str],
    image_filepaths: List[str],
    coco_txt_filepaths: List[str],
    sync_config: Optional[TrainingSyncConfig],
    task: Literal["detect", "segment", "pose"],
):
    training_results = None
    traceback_logs = None

    def _on_failure(logs: str) -> None:
        nonlocal traceback_logs
        traceback_logs = logs
        logger.error("%s", logs)

    training_results, traceback_logs = run_training_subprocess_body(
        action=lambda: train_model(
            yolov8_training_config=yolov8_training_config,
            objects_count=objects_count,
            class_names_in=class_names,
            image_filepaths=image_filepaths,
            coco_txt_filepaths=coco_txt_filepaths,
            sync_config=sync_config,
            task=task,
        ),
        on_failure=_on_failure,
    )
    train_model_result = TrainModelResult(training_results=training_results, traceback_logs=traceback_logs)
    failed = traceback_logs is not None or training_results is None
    logger.info("Training process exited!")
    finish_training_subprocess(queue, train_model_result, failed=failed)
