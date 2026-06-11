import importlib.util
import json
import logging
import multiprocessing as mp
import os
import shutil
import subprocess
import sys
import tempfile
from contextlib import nullcontext
from dataclasses import dataclass, field, fields
from pathlib import Path
from typing import List, Optional, Union

import numpy as np
import yaml
from PIL import Image

from datapipe_ml.frameworks.yolo.artifacts import (
    YoloDataYAMLConfig,
    yolo_collect_results_generic,
    yolo_copy_back_and_cleanup,
    yolo_count_objects,
    yolo_load_best_threshold_from_curve_csv,
    yolo_load_data_config,
    yolo_prepare_tmp_dirs_for_cloud_yolov5,
    yolo_select_last_exp,
    yolo_write_data_yaml_if_needed,
)
from datapipe_ml.training.specs import TrainingSyncConfig
from datapipe_ml.training.sync import PeriodicTrainingSync

logger = logging.getLogger("datapipe.ml.yolov5.script")
os.environ["WANDB_DISABLED"] = "true"  # запретить wandb

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
    resume: bool = False  # resume most recent training
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
    tmp_folder: str = "/tmp/"  # When used cloud images, store them to this folder
    initial_weights_path: Optional[str] = None

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
        skip_fields = {"tmp_folder", "initial_weights_path", *YOLOV5_HYP_OVERRIDE_FIELD_NAMES}
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
        raise ValueError("YOLOv5 package is not installed. Install the 'YOLOv5' package or pass yolov5_script_file.")

    train_script = Path(next(iter(package_spec.submodule_search_locations))) / "train.py"
    if not train_script.exists():
        raise ValueError(f"Installed YOLOv5 package does not contain train.py at {train_script}.")
    return train_script


def _resolve_yolov5_root_expr(value: str, root: Path) -> Optional[str]:
    for prefix in ("yolov5.ROOT / '", "yolov5_module.ROOT / '"):
        if value.startswith(prefix) and value.endswith("'"):
            return str(root / value[len(prefix) : -1])
    return None


def _apply_yolov5_hyp_overrides(cfg: YoloV5_TrainingConfig) -> None:
    overrides = {
        field.name: cfg.__dict__[field.name]
        for field in fields(cfg)
        if field.name in YOLOV5_HYP_OVERRIDE_FIELD_NAMES and cfg.__dict__[field.name] is not None
    }
    if not overrides:
        return

    hyp_path = Path(cfg.hyp)
    with open(hyp_path) as f:
        hyp = yaml.safe_load(f) or {}
    hyp.update(overrides)

    tmp_hyp = tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False)
    yaml.safe_dump(hyp, tmp_hyp, sort_keys=False)
    tmp_hyp.close()
    cfg.hyp = tmp_hyp.name
    logger.info("Applied YOLOv5 hyp overrides %s -> %s", overrides, cfg.hyp)


def _resolve_defaults_and_objects_count(cfg: YoloV5_TrainingConfig, ROOT: Path, objects_count: Optional[int]) -> int:
    # Привязка путей по умолчанию к корню скрипта
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

    if objects_count is None:
        data_cfg = yolo_load_data_config(cfg.data) if isinstance(cfg.data, (str, Path)) else cfg.data
        objects_count = yolo_count_objects(data_cfg)

    _apply_yolov5_hyp_overrides(cfg)
    return objects_count


def train_model(
    yolov5_training_config: YoloV5_TrainingConfig,
    yolov5_script_file: Optional[Union[str, Path]],
    objects_count: Optional[int],
    class_names_in: List[str],
    image_filepaths: List[str],
    coco_txt_filepaths: List[str],
    sync_config: Optional[TrainingSyncConfig],
) -> Optional[List[TrainingResult]]:
    if yolov5_script_file is None:
        yolov5_module = resolve_installed_yolov5_train_script()
    else:
        yolov5_module = Path(yolov5_script_file)
    ROOT = yolov5_module.parent

    # Значения по умолчанию + рассчёт objects_count
    objects_count = _resolve_defaults_and_objects_count(yolov5_training_config, ROOT, objects_count)

    # Облачные пути -> локальные tmp
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

    # Если data — объект, пишем временный yaml и подменяем путь
    class_names, tmp_yaml_path = yolo_write_data_yaml_if_needed(yolov5_training_config)
    if not class_names and isinstance(yolov5_training_config.data, str):
        # если это строка — попробуем прочитать имена классов (для class_names.json)
        try:
            cfg = yolo_load_data_config(yolov5_training_config.data)
            class_names = cfg.names
        except Exception:
            class_names = []

    logger.info("yolov5_training_config=%s", yolov5_training_config)
    arguments = yolov5_training_config.get_arguments()
    command: List[str] = [sys.executable, str(yolov5_module)] + arguments
    logger.info("Running %s", command)
    env = os.environ.copy()
    env.setdefault("TORCH_FORCE_NO_WEIGHTS_ONLY_LOAD", "1")
    output_sync = None
    if sync_config is not None and sync_config.enabled:
        output_sync = PeriodicTrainingSync(
            src=str(tmp_dir_project or yolov5_training_config.project),
            dst=str(src_project_path or yolov5_training_config.project),
            config=sync_config,
            model_id=yolov5_training_config.name,
        )
    with output_sync if output_sync is not None else nullcontext():
        process = subprocess.run(command, env=env)
    logger.info("Process ended with returncode=%s.", process.returncode)
    if process.returncode != 0:
        raise RuntimeError(f"YOLOv5 training process failed with returncode={process.returncode}.")
    if yolov5_training_config.project is None:
        return None

    exp_folder = yolo_select_last_exp(yolov5_training_config.project, yolov5_training_config.name)
    if exp_folder is None:
        return None

    # приложим training_data.yaml и class_names.json
    if tmp_yaml_path and tmp_yaml_path.exists():
        shutil.copy(str(tmp_yaml_path), exp_folder / "training_data.yaml")
    with open(exp_folder / "class_names.json", "w") as out:
        json.dump(class_names, out, indent=4, ensure_ascii=False)

    f1_curve_image = None
    if (exp_folder / "F1_curve.png").exists():
        f1_curve_image = np.array(Image.open(exp_folder / "F1_curve.png"))
    best_threshold = yolo_load_best_threshold_from_curve_csv(exp_folder / "F1_curve.csv")

    # если тренировались в tmp — копируем назад в облако и чистим
    exp_folder = yolo_copy_back_and_cleanup(
        exp_folder=exp_folder,
        src_project_path=src_project_path,
        tmp_dir_images=tmp_dir_images,
        tmp_dir_project=tmp_dir_project,
        tmp_dir_images_cls=tmp_dir_images_cls,
        tmp_dir_project_cls=tmp_dir_project_cls,
        tmp_dir_model_cls=tmp_dir_model_cls,
    )

    # Сбор результатов (карта колонок -> поля dataclass)
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
        exp_folder=str(exp_folder),
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
    return results


def train_process(
    queue: mp.Queue,
    yolov5_training_config: YoloV5_TrainingConfig,
    yolov5_script_file: Optional[str],
    objects_count: int,
    class_names: List[str],
    image_filepaths: List[str],
    coco_txt_filepaths: List[str],
    sync_config: Optional[TrainingSyncConfig],
) -> List[TrainingResult]:
    training_results = None
    traceback_logs = None
    try:
        training_results = train_model(
            yolov5_training_config=yolov5_training_config,
            yolov5_script_file=yolov5_script_file,
            objects_count=objects_count,
            class_names_in=class_names,
            image_filepaths=image_filepaths,
            coco_txt_filepaths=coco_txt_filepaths,
            sync_config=sync_config,
        )
    except Exception as e:
        from traceback_with_variables import format_exc

        traceback_logs = format_exc(e)
        logger.error("%s", traceback_logs)
        logger.error("Training model failed.")
    finally:
        train_model_result = TrainModelResult(training_results=training_results, traceback_logs=traceback_logs)
        queue.put(train_model_result)
        logger.info("Training process exited!")
        exit(0)
