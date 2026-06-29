import importlib.util
import json
import logging
import multiprocessing as mp
import os
import shutil
import sys
import tempfile
from dataclasses import dataclass, field, fields
from pathlib import Path
from typing import List, Optional, Union

import numpy as np
import yaml
from PIL import Image

from datapipe_ml.core.multiprocessing import finish_training_subprocess
from datapipe_ml.core.training_subprocess import run_training_subprocess_body
from datapipe_ml.training.paths import default_tmp_folder
from datapipe_ml.frameworks.yolo.artifacts import (
    YoloDataYAMLConfig,
    yolo_collect_results_generic,
    yolo_finalize_training_output,
    yolo_count_objects,
    yolo_load_best_threshold_from_curve_csv,
    yolo_load_data_config,
    yolo_persisted_exp_folder,
    yolo_prepare_tmp_dirs_for_cloud_yolov5,
    yolo_remap_collected_model_paths,
    yolo_select_last_exp,
)
from datapipe_ml.frameworks.yolo.train_session import (
    YoloTrainSession,
    yolo_write_exp_metadata,
)
from datapipe_ml.frameworks.yolo.yolov5.train_in_process import run_yolov5_train_in_process
from datapipe_ml.training.specs import TrainingSyncConfig

logger = logging.getLogger("datapipe.ml.yolov5.script")

YOLOV5_DEFAULT_PROJECT = "yolov5.ROOT / 'runs/train'"
YOLOV5_DEFAULT_DATA = "yolov5.ROOT / 'data/coco128.yaml'"
YOLOV5_DEFAULT_CFG = "yolov5.ROOT / 'models/yolov5s.yaml'"
YOLOV5_DEFAULT_HYP = "yolov5.ROOT / 'data/hyps/hyp.scratch-low.yaml'"

# Optional hyp.yaml overrides (not YOLOv5 CLI args); see YOLOv5 data/hyps/*.yaml
YOLOV5_HYP_OVERRIDE_FIELD_NAMES = (
    "hsv_h",
    "hsv_s",
    "hsv_v",
    "degrees",
    "translate",
    "scale",
    "shear",
    "perspective",
    "flipud",
    "fliplr",
    "mosaic",
    "mixup",
    "copy_paste",
)


def _yolov5_cli_name(field_name: str) -> str:
    if field_name in ["upload_dataset", "artifact_alias"]:
        return field_name
    return field_name.replace("_", "-")


def _should_skip_yolov5_cli_value(field_name: str, value: object) -> bool:
    if value is None:
        return True
    if isinstance(value, bool):
        return not value
    if isinstance(value, str):
        return field_name != "weights" and value == ""
    return field_name != "batch_size" and value == -1


@dataclass
class YoloV5_TrainingConfig:
    project: Optional[str] = YOLOV5_DEFAULT_PROJECT  # also known as 'logdir', save to project/name
    name: str = "exp"  # save to project/name, f. e. 'exp'
    weights: str = "yolov5s.pt"  # initial weights path
    data: Optional[Union[str, YoloDataYAMLConfig]] = YOLOV5_DEFAULT_DATA
    imgsz: int = 640  # train, val image size (pixels)
    batch_size: int = 16  # total batch size for all GPUs, -1 for autobatch
    epochs: int = 300  # number of epochs
    save_period: int = -1  # Save checkpoint every x epochs (disabled if < 1)
    cfg: str = ""  # model.yaml path
    hyp: str = YOLOV5_DEFAULT_HYP  # hyperparameters path (ROOT relative)
    rect: bool = False  # rectangular training
    resume: Union[bool, str] = False  # resume most recent training or checkpoint path
    nosave: bool = False  # only save final checkpoint
    noval: bool = False  # only validate final epoch
    noautoanchor: bool = False  # disable AutoAnchor
    noplots: bool = False  # save no plot files
    evolve: Optional[int] = None  # evolve hyperparameters for x generations
    bucket: str = ""  # gsutil bucket
    cache: Optional[str] = None  # cache images in "ram" (default) or "disk"
    image_weights: bool = False  # use weighted image selection for training
    device: str = ""  # cuda device, i.e. 0 or 0,1,2,3 or cpu
    multi_scale: bool = False  # vary img-size +/- 50percent
    single_cls: bool = False  # train multi-class data as single-class
    optimizer: str = "SGD"  # optimizer (possible: 'SGD', 'Adam', 'AdamW')
    sync_bn: bool = False  # use SyncBatchNorm, only available in DDP mode
    workers: int = 8  # max dataloader workers (per RANK in DDP mode)
    exist_ok: bool = False  # existing project/name ok, do not increment
    quad: bool = False  # quad dataloader
    cos_lr: bool = False  # cosine LR scheduler
    label_smoothing: float = 0.0  # Label smoothing epsilon
    patience: int = 100  # EarlyStopping patience (epochs without improvement)
    freeze: List[int] = field(default_factory=lambda: [0])  # Freeze layers: backbone=10, first3=0 1 2
    local_rank: int = -1  # DDP parameter, do not modify
    seed: int = 0  # Global training seed
    # Weights & Biases arguments
    entity: Optional[str] = None  # W&B: Entity
    upload_dataset: bool = False  # W&B: Upload data, "val" option
    bbox_interval: int = -1  # W&B: Set bounding-box image logging interval
    artifact_alias: str = "latest"  # W&B: Version of dataset artifact to use

    # Mine Epoch8 arguments:
    tmp_folder: str = field(default_factory=default_tmp_folder)  # When used cloud images, store them to this folder
    initial_weights_path: Optional[str] = None
    persisted_project_dir: Optional[str] = None

    # Optional hyp.yaml augmentation overrides (None = use value from hyp file)
    hsv_h: Optional[float] = None  # HSV-Hue augmentation (fraction)
    hsv_s: Optional[float] = None  # HSV-Saturation augmentation (fraction)
    hsv_v: Optional[float] = None  # HSV-Value augmentation (fraction)
    degrees: Optional[float] = None  # Image rotation (+/- deg)
    translate: Optional[float] = None  # Image translation (+/- fraction)
    scale: Optional[float] = None  # Image scale (+/- gain)
    shear: Optional[float] = None  # Image shear (+/- deg)
    perspective: Optional[float] = None  # Image perspective (+/- fraction), range 0–0.001
    flipud: Optional[float] = None  # Flip up-down probability
    fliplr: Optional[float] = None  # Flip left-right probability
    mosaic: Optional[float] = None  # Mosaic augmentation probability
    mixup: Optional[float] = None  # MixUp augmentation probability
    copy_paste: Optional[float] = None  # Copy-paste augmentation probability (segmentation)

    def get_arguments(self) -> List[str]:
        arguments = []
        skip_fields = {"tmp_folder", "initial_weights_path", "persisted_project_dir", *YOLOV5_HYP_OVERRIDE_FIELD_NAMES}
        for field_ in fields(self):
            if field_.name in skip_fields:
                continue
            value = self.__getattribute__(field_.name)
            if _should_skip_yolov5_cli_value(field_.name, value):
                continue
            name = _yolov5_cli_name(field_.name)
            if isinstance(value, bool):
                arguments.append(f"--{name}")
                continue
            if isinstance(value, list):
                arguments.extend([f"--{name}", *[str(x) for x in value]])
                continue
            arguments.extend([f"--{name}", str(value)])
        return arguments


@dataclass
class TrainingResult:
    detection_model_id: str
    class_names: List[str]
    epoch: int
    model_path: Optional[str]
    train_box_loss: float
    train_obj_loss: float
    train_cls_loss: float
    metrics_precision: float
    metrics_recall: float
    metrics_mAP_0_5: float
    metrics_mAP_0_5_to_0_95: float
    val_box_loss: float
    val_obj_loss: float
    val_cls_loss: float
    x_lr0: float
    x_lr1: float
    x_lr2: float
    objects_count: Optional[int]
    f1_curve_image: Optional[np.ndarray]
    best_threshold: Optional[float]


@dataclass
class TrainModelResult:
    training_results: Optional[List[TrainingResult]] = None
    traceback_logs: Optional[str] = None


def resolve_installed_yolov5_train_script() -> Path:
    package_spec = importlib.util.find_spec("yolov5")
    if package_spec is None or package_spec.submodule_search_locations is None:
        raise ValueError(
            "YOLOv5 package is not installed. Install the optional 'torch' extra: pip install 'datapipe-ml[torch]'."
        )

    train_script = Path(next(iter(package_spec.submodule_search_locations))) / "train.py"
    if not train_script.exists():
        raise ValueError(f"Installed YOLOv5 package does not contain train.py at {train_script}.")
    return train_script


def _resolve_yolov5_root_expr(value: str, root: Path) -> Optional[str]:
    for prefix in ("yolov5.ROOT / '", "yolov5_module.ROOT / '"):
        if value.startswith(prefix) and value.endswith("'"):
            return str(root / value[len(prefix) : -1])
    return None


def _apply_yolov5_hyp_overrides(cfg: YoloV5_TrainingConfig) -> Optional[Path]:
    overrides = {
        field.name: cfg.__dict__[field.name]
        for field in fields(cfg)
        if field.name in YOLOV5_HYP_OVERRIDE_FIELD_NAMES and cfg.__dict__[field.name] is not None
    }
    if not overrides:
        return None

    hyp_path = Path(cfg.hyp)
    with open(hyp_path) as f:
        hyp = yaml.safe_load(f) or {}
    hyp.update(overrides)

    tmp_hyp_path = Path(tempfile.mkstemp(suffix=".yaml")[1])
    with open(tmp_hyp_path, "w") as tmp_hyp:
        yaml.safe_dump(hyp, tmp_hyp, sort_keys=False)
    cfg.hyp = str(tmp_hyp_path)
    logger.info("Applied YOLOv5 hyp overrides %s -> %s", overrides, cfg.hyp)
    return tmp_hyp_path


def _resolve_defaults_and_objects_count(
    cfg: YoloV5_TrainingConfig, ROOT: Path, objects_count: Optional[int]
) -> tuple[int, Optional[Path]]:
    # Bind default paths to the training script root.
    if cfg.project is None:
        cfg.project = str(ROOT / "runs/train")
    else:
        resolved_project = _resolve_yolov5_root_expr(cfg.project, ROOT)
        if resolved_project is not None:
            cfg.project = resolved_project

    if cfg.data is None:
        cfg.data = str(ROOT / "data/coco128.yaml")
    elif isinstance(cfg.data, str):
        resolved_data = _resolve_yolov5_root_expr(cfg.data, ROOT)
        if resolved_data is not None:
            cfg.data = resolved_data

    resolved_cfg = _resolve_yolov5_root_expr(cfg.cfg, ROOT)
    if resolved_cfg is not None:
        cfg.cfg = resolved_cfg

    if cfg.hyp is None:
        cfg.hyp = str(ROOT / "data/hyps/hyp.scratch-low.yaml")
    else:
        resolved_hyp = _resolve_yolov5_root_expr(cfg.hyp, ROOT)
        if resolved_hyp is not None:
            cfg.hyp = resolved_hyp

    tmp_hyp_path = _apply_yolov5_hyp_overrides(cfg)
    if objects_count is None:
        data_cfg = yolo_load_data_config(cfg.data) if isinstance(cfg.data, (str, Path)) else cfg.data
        objects_count = yolo_count_objects(data_cfg)

    return objects_count, tmp_hyp_path


def train_model(
    yolov5_training_config: YoloV5_TrainingConfig,
    objects_count: Optional[int],
    class_names_in: List[str],
    image_filepaths: List[str],
    coco_txt_filepaths: List[str],
    sync_config: Optional[TrainingSyncConfig],
) -> Optional[List[TrainingResult]]:
    yolov5_module = resolve_installed_yolov5_train_script()
    ROOT = yolov5_module.parent

    # Defaults plus objects_count calculation.
    objects_count, tmp_hyp_path = _resolve_defaults_and_objects_count(yolov5_training_config, ROOT, objects_count)

    # Cloud paths -> local temp copies.
    (
        src_images_dir_path,
        src_project_path,
        tmp_dir_images,
        tmp_dir_project,
        tmp_dir_images_cls,
        tmp_dir_project_cls,
        tmp_dir_model_cls,
    ) = yolo_prepare_tmp_dirs_for_cloud_yolov5(
        yolov5_training_config,
        image_filepaths=image_filepaths,
        coco_txt_filepaths=coco_txt_filepaths,
    )

    session = YoloTrainSession(
        training_config=yolov5_training_config,
        sync_config=sync_config,
        src_project_path=src_project_path,
        tmp_dir_project=tmp_dir_project,
        extra_temp_paths=(tmp_hyp_path,),
    )

    def _launch(class_names: List[str], tmp_yaml_path: Optional[Path]) -> Optional[Path]:
        logger.info("yolov5_training_config=%s", yolov5_training_config)
        arguments = yolov5_training_config.get_arguments()
        logger.info("Running in-process YOLOv5 train with arguments %s", arguments)
        run_yolov5_train_in_process(yolov5_module, arguments)
        if yolov5_training_config.project is None:
            return None
        selected_exp_folder = yolo_select_last_exp(yolov5_training_config.project, yolov5_training_config.name)
        if selected_exp_folder is None:
            return None
        yolo_write_exp_metadata(selected_exp_folder, tmp_yaml_path, class_names)
        return selected_exp_folder

    exp_folder, class_names = session.run(_launch)

    if exp_folder is None:
        return None

    f1_curve_image = None
    if (exp_folder / "F1_curve.png").exists():
        f1_curve_image = np.array(Image.open(exp_folder / "F1_curve.png"))
    best_threshold = yolo_load_best_threshold_from_curve_csv(exp_folder / "F1_curve.csv")

    local_exp_folder = exp_folder
    persisted_project = yolov5_training_config.persisted_project_dir or src_project_path
    persisted_exp = yolo_persisted_exp_folder(persisted_project, local_exp_folder) if persisted_project else None

    # Collect from the local training output (complete and stable); remote mirrors may lag.
    rename_map = {
        "epoch": "epoch",
        "train/box_loss": "train_box_loss",
        "train/obj_loss": "train_obj_loss",
        "train/cls_loss": "train_cls_loss",
        "metrics/precision": "metrics_precision",
        "metrics/recall": "metrics_recall",
        "metrics/mAP_0.5": "metrics_mAP_0_5",
        "metrics/mAP_0.5:0.95": "metrics_mAP_0_5_to_0_95",
        "val/box_loss": "val_box_loss",
        "val/obj_loss": "val_obj_loss",
        "val/cls_loss": "val_cls_loss",
        "x/lr0": "x_lr0",
        "x/lr1": "x_lr1",
        "x/lr2": "x_lr2",
    }
    results = yolo_collect_results_generic(
        exp_folder=str(local_exp_folder),
        result_cls=TrainingResult,
        id_field_name="detection_model_id",
        id_field_value=yolov5_training_config.name,
        class_names=class_names,
        objects_count=objects_count,
        f1_image_field_name="f1_curve_image",
        f1_image=f1_curve_image,
        best_threshold=best_threshold,
        rename_map=rename_map,
        best_metric_col="metrics_mAP_0_5_to_0_95",
        weights_subdir="weights",
    )
    if persisted_exp is not None:
        yolo_remap_collected_model_paths(
            results,
            local_exp_root=str(local_exp_folder),
            persisted_exp_root=persisted_exp,
        )

    yolo_finalize_training_output(
        local_exp_folder,
        persisted_project_dir=persisted_project,
        tmp_dir_images_cls=tmp_dir_images_cls,
        tmp_dir_project_cls=tmp_dir_project_cls,
        tmp_dir_model_cls=tmp_dir_model_cls,
    )
    return results


def train_process(
    queue: mp.Queue,
    yolov5_training_config: YoloV5_TrainingConfig,
    objects_count: int,
    class_names: List[str],
    image_filepaths: List[str],
    coco_txt_filepaths: List[str],
    sync_config: Optional[TrainingSyncConfig],
) -> None:
    training_results = None
    traceback_logs = None

    def _on_failure(logs: str) -> None:
        nonlocal traceback_logs
        traceback_logs = logs
        logger.error("%s", logs)

    training_results, traceback_logs = run_training_subprocess_body(
        action=lambda: train_model(
            yolov5_training_config=yolov5_training_config,
            objects_count=objects_count,
            class_names_in=class_names,
            image_filepaths=image_filepaths,
            coco_txt_filepaths=coco_txt_filepaths,
            sync_config=sync_config,
        ),
        on_failure=_on_failure,
    )
    train_model_result = TrainModelResult(training_results=training_results, traceback_logs=traceback_logs)
    failed = traceback_logs is not None or training_results is None
    logger.info("Training process exited!")
    finish_training_subprocess(queue, train_model_result, failed=failed)
