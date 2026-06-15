from __future__ import annotations

import tempfile
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import (
    Any,
    Dict,
    List,
    Literal,
    Optional,
    Protocol,
    Sequence,
    Tuple,
    Type,
    Union,
    cast,
    runtime_checkable,
)

import fsspec
import numpy as np
import pandas as pd
import yaml
from pathy import FluidPath, Pathy

from datapipe_ml.training.staging import (
    LocalStagingDir,
    copy_file_to_local_staging,
    make_local_staging_dir,
    stage_files_to_local_folder,
)


class YoloTrainingConfigForCloud(Protocol):
    data: Optional[Union[str, "YoloDataYAMLConfig"]]
    project: Optional[str]
    name: str
    tmp_folder: str
    initial_weights_path: Optional[str]


class YoloV5TrainingConfigForCloud(YoloTrainingConfigForCloud, Protocol):
    weights: str


class YoloV8TrainingConfigForCloud(YoloTrainingConfigForCloud, Protocol):
    model: str


@dataclass
class YoloDataYAMLConfig:
    path: str
    train: str
    val: str
    test: Optional[str]
    nc: int
    names: List[str]
    kpt_shape: Optional[List[int]] = None
    flip_idx: Optional[List[int]] = None


def yolo_load_data_config(data: Union[str, YoloDataYAMLConfig]) -> YoloDataYAMLConfig:
    if isinstance(data, YoloDataYAMLConfig):
        return data
    with fsspec.open(str(data), "r") as src:
        raw = yaml.safe_load(src)
    return YoloDataYAMLConfig(**raw)


def yolo_count_objects(cfg: YoloDataYAMLConfig) -> int:
    p = Path(cfg.path)
    train_cnt = len(list((p / cfg.train / "labels").glob("*.txt")))
    val_cnt = len(list((p / cfg.val / "labels").glob("*.txt")))
    test_cnt = len(list((p / cfg.test / "labels").glob("*.txt"))) if cfg.test else 0
    return train_cnt + val_cnt + test_cnt


def yolo_best_threshold_from_curve(
    x: Sequence[float],
    y: Union[Sequence[Sequence[float]], np.ndarray],
    *,
    min_threshold: float = 0.3,
) -> float:
    """Return the confidence threshold used by YOLO F1-confidence plots."""
    best_threshold = 0.45
    try:
        x_array = np.asarray(x, dtype=float)
        y_array = np.asarray(y, dtype=float)
        if x_array.size == 0 or y_array.size == 0:
            return best_threshold

        if y_array.ndim == 1:
            y_mean = y_array
        else:
            y_mean = y_array.mean(axis=0)
        if y_mean.size != x_array.size:
            return best_threshold

        # Mirrors Ultralytics' plot_mc_curve/ap_per_class logic for the "all classes" F1 curve.
        best_threshold = max(min_threshold, float(x_array[int(_yolo_smooth(y_mean, 0.1).argmax())]))
    except KeyboardInterrupt:
        raise
    except Exception:
        print("get_best_threshold: skipping due to exception")
    return best_threshold


def _yolo_smooth(y: np.ndarray, f: float = 0.05) -> np.ndarray:
    nf = round(len(y) * f * 2) // 2 + 1
    p = np.ones(nf // 2)
    yp = np.concatenate((p * y[0], y, p * y[-1]), 0)
    return np.convolve(yp, np.ones(nf) / nf, mode="valid")


_YoloCurveValues = Sequence[Union[Sequence[float], np.ndarray]]


@runtime_checkable
class _UltralyticsCurveMetrics(Protocol):
    curves: Sequence[str]
    curves_results: Sequence[_YoloCurveValues]


def _curve_xy_from_ultralytics_values(values: Any) -> tuple[Any, Any] | None:
    if not isinstance(values, (tuple, list)) or len(values) < 2:
        return None
    return values[0], values[1]


def yolo_best_threshold_from_ultralytics_metrics(metrics: Any, curve_name_part: str = "F1-Confidence") -> float:
    best_threshold = 0.45
    if not isinstance(metrics, _UltralyticsCurveMetrics):
        return best_threshold
    curves = metrics.curves
    curves_results = metrics.curves_results
    if not curves or not curves_results:
        return best_threshold

    for name, values in zip(curves, curves_results):
        if curve_name_part not in str(name):
            continue
        xy = _curve_xy_from_ultralytics_values(values)
        if xy is None:
            continue
        return yolo_best_threshold_from_curve(xy[0], xy[1])
    return best_threshold


def yolo_load_best_threshold_from_curve_csv(path: Union[str, Path, Pathy]) -> float:
    best_threshold = 0.45
    try:
        from datapipe_ml.utils.fsspec_storage import fsspec_storage_options

        pathy = Pathy.fluid(str(path))
        path_str = str(pathy)
        storage_options = fsspec_storage_options(path_str)
        if not fsspec.open(path_str, **storage_options).fs.exists(path_str):
            return best_threshold
        with fsspec.open(path_str, "r", **storage_options) as src:
            df = pd.read_csv(src)
        if "best_threshold" not in df.columns or df.empty:
            return best_threshold
        return max(0.3, float(cast(Any, df.loc[0, "best_threshold"])))
    except KeyboardInterrupt:
        raise
    except Exception:
        print("get_best_threshold: skipping due to exception")
    return best_threshold


def yolo_write_data_yaml_if_needed(training_config) -> Tuple[List[str], Path]:
    class_names: List[str] = []
    if isinstance(training_config.data, YoloDataYAMLConfig):
        tmp = tempfile.NamedTemporaryFile(suffix=".yaml", delete=False)
        tmp_path = Path(tmp.name)
        data_yaml = {key: value for key, value in asdict(training_config.data).items() if value is not None}
        yaml_config = yaml.dump(data_yaml, allow_unicode=True)
        with fsspec.open(str(tmp_path), "w") as out:
            out.write(yaml_config)
        class_names = training_config.data.names
        training_config.data = str(tmp_path)
        return class_names, tmp_path
    else:
        return class_names, Path("")  # ничего не писали


def yolo_select_last_exp(project: str, name_prefix: str) -> Optional[Path]:
    exps = sorted(Path(project).glob(f"{name_prefix}*"), key=lambda p: p.stat().st_mtime)
    return exps[-1] if exps else None


def yolo_prepare_tmp_dirs_for_cloud_yolov5(
    training_config: YoloV5TrainingConfigForCloud,
    image_filepaths: List[str],
    coco_txt_filepaths: List[str],
) -> Tuple[
    Optional[YoloDataYAMLConfig],
    Optional[str],
    Optional[str],
    Optional[str],
    Optional[LocalStagingDir],
    Optional[LocalStagingDir],
    Optional[LocalStagingDir],
]:
    return _yolo_prepare_tmp_dirs_for_cloud(
        training_config=training_config,
        image_filepaths=image_filepaths,
        coco_txt_filepaths=coco_txt_filepaths,
        model_weight_field="weights",
    )


def yolo_prepare_tmp_dirs_for_cloud_yolov8(
    training_config: YoloV8TrainingConfigForCloud,
    image_filepaths: List[str],
    coco_txt_filepaths: List[str],
) -> Tuple[
    Optional[YoloDataYAMLConfig],
    Optional[str],
    Optional[str],
    Optional[str],
    Optional[LocalStagingDir],
    Optional[LocalStagingDir],
    Optional[LocalStagingDir],
]:
    return _yolo_prepare_tmp_dirs_for_cloud(
        training_config=training_config,
        image_filepaths=image_filepaths,
        coco_txt_filepaths=coco_txt_filepaths,
        model_weight_field="model",
    )


def _yolo_prepare_tmp_dirs_for_cloud(
    training_config: Union[YoloV5TrainingConfigForCloud, YoloV8TrainingConfigForCloud],
    image_filepaths: List[str],
    coco_txt_filepaths: List[str],
    model_weight_field: Literal["weights", "model"],
) -> Tuple[
    Optional[YoloDataYAMLConfig],
    Optional[str],
    Optional[str],
    Optional[str],
    Optional[LocalStagingDir],
    Optional[LocalStagingDir],
    Optional[LocalStagingDir],
]:
    assert isinstance(training_config.data, YoloDataYAMLConfig)
    tmp_folder = training_config.tmp_folder
    images_dir_path = training_config.data
    project_path = training_config.project
    protocol_images, _ = fsspec.core.split_protocol(training_config.data.path)
    protocol_project, _ = fsspec.core.split_protocol(training_config.project)

    tmp_dir_images = None
    tmp_dir_images_cls = None
    if not (protocol_images is None or protocol_images == "file"):
        tmp_dir_images_cls = make_local_staging_dir(
            tmp_folder=tmp_folder,
            name=f"tmp_images_{training_config.name}",
            use_managed_tmp=tmp_folder == "/tmp/",
            remove_on_cleanup=True,
        )
        tmp_dir_images = str(tmp_dir_images_cls.path)
        stage_files_to_local_folder(
            image_filepaths + coco_txt_filepaths,
            tmp_dir_images_cls.path,
            label="YOLO dataset files",
        )
        training_config.data.path = tmp_dir_images

    tmp_dir_model_cls = None
    weight_path = training_config.initial_weights_path
    if weight_path is None:
        if model_weight_field == "weights":
            weight_path = cast(YoloV5TrainingConfigForCloud, training_config).weights
        else:
            weight_path = cast(YoloV8TrainingConfigForCloud, training_config).model

    if weight_path is not None:
        protocol_weights, _ = fsspec.core.split_protocol(str(weight_path))
        if not (protocol_weights is None or protocol_weights == "file"):
            tmp_dir_model_cls = make_local_staging_dir(
                tmp_folder=tmp_folder,
                name=f"tmp_model_{training_config.name}",
                use_managed_tmp=tmp_folder == "/tmp/",
                remove_on_cleanup=True,
            )
            model_path = copy_file_to_local_staging(
                str(weight_path),
                tmp_dir_model_cls,
                label="YOLO initial weights",
            )
            _set_training_weight_path(training_config, str(model_path), model_weight_field)
        elif training_config.initial_weights_path is not None:
            _set_training_weight_path(training_config, str(training_config.initial_weights_path), model_weight_field)

    tmp_dir_project = None
    tmp_dir_project_cls = None
    if not (protocol_project is None or protocol_project == "file"):
        tmp_dir_project_cls = make_local_staging_dir(
            tmp_folder=tmp_folder,
            name=f"tmp_project_{training_config.name}",
            use_managed_tmp=tmp_folder == "/tmp/",
            remove_on_cleanup=True,
        )
        tmp_dir_project = str(tmp_dir_project_cls.path)
        training_config.project = tmp_dir_project

    return (
        images_dir_path,
        project_path,
        tmp_dir_images,
        tmp_dir_project,
        tmp_dir_images_cls,
        tmp_dir_project_cls,
        tmp_dir_model_cls,
    )


def _set_training_weight_path(
    training_config: Union[YoloV5TrainingConfigForCloud, YoloV8TrainingConfigForCloud],
    model_path: str,
    model_weight_field: Literal["weights", "model"],
) -> None:
    if model_weight_field == "weights":
        cast(YoloV5TrainingConfigForCloud, training_config).weights = model_path
    else:
        cast(YoloV8TrainingConfigForCloud, training_config).model = model_path


def yolo_finalize_training_output(
    exp_folder: Path,
    *,
    persisted_project_dir: Optional[str],
    tmp_dir_images_cls: Optional[LocalStagingDir],
    tmp_dir_project_cls: Optional[LocalStagingDir],
    tmp_dir_model_cls: Optional[LocalStagingDir],
) -> FluidPath:
    if persisted_project_dir and tmp_dir_project_cls is not None:
        from datapipe_ml.training.sync import copy_tree_snapshot

        dst_exp = str(Pathy.fluid(persisted_project_dir) / exp_folder.name)
        copy_tree_snapshot(str(exp_folder), dst_exp)
        final_exp_folder: FluidPath = Pathy.fluid(dst_exp)
    elif persisted_project_dir:
        final_exp_folder = Pathy.fluid(persisted_project_dir) / exp_folder.name
    else:
        final_exp_folder = Pathy.fluid(str(exp_folder))

    for staging_dir in (tmp_dir_images_cls, tmp_dir_project_cls, tmp_dir_model_cls):
        if staging_dir is not None:
            staging_dir.cleanup()

    return final_exp_folder


def yolo_copy_back_and_cleanup(
    exp_folder: Path,
    src_project_path: Optional[str],
    tmp_dir_images: Optional[str],
    tmp_dir_project: Optional[str],
    tmp_dir_images_cls: Optional[LocalStagingDir],
    tmp_dir_project_cls: Optional[LocalStagingDir],
    tmp_dir_model_cls: Optional[LocalStagingDir],
    output_synced: bool = False,
) -> FluidPath:
    del tmp_dir_images, tmp_dir_project, output_synced
    return yolo_finalize_training_output(
        exp_folder,
        persisted_project_dir=src_project_path,
        tmp_dir_images_cls=tmp_dir_images_cls,
        tmp_dir_project_cls=tmp_dir_project_cls,
        tmp_dir_model_cls=tmp_dir_model_cls,
    )


def yolo_collect_results_generic(
    *,
    exp_folder: Union[str, Path, Pathy],
    result_cls: Type[Any],
    id_field_name: str,
    id_field_value: str,
    class_names: List[str],
    objects_count: Optional[int],
    f1_image_field_name: str,
    f1_image: Optional[np.ndarray],
    best_threshold: Optional[float],
    rename_map: Dict[str, str],
    best_metric_col: str,
    weights_subdir: str = "weights",
) -> List[Any]:
    from datapipe_ml.utils.fsspec_storage import fsspec_storage_options

    exp_pathy = Pathy.fluid(str(exp_folder))
    df_results_path = exp_pathy / "results.csv"
    df_results_path_str = str(df_results_path)
    storage_options = fsspec_storage_options(df_results_path_str)
    with fsspec.open(df_results_path_str, "r", **storage_options) as src:
        df = pd.read_csv(src, skipinitialspace=True)

    df = df.rename(columns=rename_map)
    if "epoch" not in df.columns:
        raise ValueError("results.csv must contain 'epoch' column (after rename)")

    last_epoch = int(float(cast(Any, df["epoch"].max())))
    if best_metric_col not in df.columns:
        fallback = "metrics_mAP_0_5_to_0_95"
        if fallback in df.columns:
            best_metric_col = fallback
        else:
            best_epoch = last_epoch
    else:
        best_epoch = int(float(cast(Any, df.loc[int(df[best_metric_col].argmax()), "epoch"])))

    results: List[Any] = []
    ann = result_cls.__annotations__
    for idx in df.index:
        epoch = int(float(cast(Any, df.loc[idx, "epoch"])))
        model_path = Pathy.fluid(str(exp_pathy / weights_subdir / f"epoch{epoch}.pt"))
        model_path_str = str(model_path)
        filesystem = fsspec.open(model_path_str, **storage_options).fs
        if not filesystem.exists(model_path_str):
            if epoch == best_epoch:
                model_path = exp_pathy / weights_subdir / "best.pt"
            elif epoch == last_epoch:
                model_path = exp_pathy / weights_subdir / "last.pt"
            else:
                model_path = exp_pathy / weights_subdir / "best.pt"

        special_fields = [
            id_field_name,
            "class_names",
            "model_path",
            f1_image_field_name,
            "best_threshold",
            "objects_count",
        ]
        training_values = {k: v for k, v in dict(df.loc[idx]).items() if k in ann and k not in special_fields}
        missing_values = {k: None for k in ann if k not in training_values and k not in special_fields}

        payload = {
            id_field_name: str(id_field_value),
            "class_names": class_names,
            "model_path": str(model_path),
            f1_image_field_name: (f1_image if epoch == best_epoch else None),
            "best_threshold": best_threshold if epoch == best_epoch else None,
            "objects_count": objects_count,
            **training_values,
            **missing_values,
        }
        results.append(result_cls(**payload))  # type: ignore

    return results
