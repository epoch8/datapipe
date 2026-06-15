from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Tuple, Type, TypeVar, cast

import pandas as pd
from datapipe.datatable import DataTable
from datapipe.types import IndexDF
from pathy import Pathy

from datapipe_ml.frameworks.yolo.artifacts import YoloDataYAMLConfig
from datapipe_ml.frameworks.yolo.checkpoint_label import build_yolo_train_config_summary
from datapipe_ml.training.train_config_id import build_train_config_id
from datapipe_ml.training.specs import (
    Algo,
    PreparedData,
    TrainContext,
    TrainingLaunchRequest,
    TrainingPathMap,
    TrainingResumeConfig,
    TrainingSyncConfig,
    build_training_launcher,
)

if TYPE_CHECKING:
    from datapipe_ml.frameworks.yolo.yolov5.runner import TrainModelResult as YoloV5TrainModelResult
    from datapipe_ml.frameworks.yolo.yolov8.runner import TrainModelResult as YoloV8TrainModelResult


def get_best_models(df_models: pd.DataFrame, model_id_column: str) -> pd.DataFrame:
    """
    input: ["model_id", "epoch", "model_path", "metrics...", "best_threshold", ...]
    output: ["model_id", "model_path", "metrics..", "best_threshold"]
    """
    df_results = []
    for _, df_models_by_name in df_models.groupby(model_id_column):
        # https://github.com/ultralytics/yolov8/issues/8701
        df_models_by_name["fitness"] = df_models_by_name.apply(
            lambda row: 0.1 * row["metrics_mAP_0_5"] + 0.9 * row["metrics_mAP_0_5_to_0_95"], axis=1
        )
        best_idx = int(df_models_by_name["fitness"].argmax())
        df_results.append(df_models_by_name.iloc[[best_idx]][[model_id_column, "class_names", "model_path"]])
    return pd.concat(df_results, ignore_index=True)


@dataclass
class YoloPreparedData(PreparedData):
    data_src_path: str
    objects_count: int
    image_filepaths: List[str]
    yolo_txt_filepaths: List[str]
    images_data_paths_train: List[str]
    images_data_paths_val: List[str]
    images_data_paths_test: List[str]
    class_names: List[str]


@dataclass
class YoloTrainContext(TrainContext):
    dt__class_names: DataTable
    dt__resized_image_file: DataTable
    dt__yolo_txt: DataTable
    ignore_errors_sample_sizes: bool
    extra_class_names_to_yaml_fields: List[str] = field(default_factory=list)


YoloContextT = TypeVar("YoloContextT", bound=YoloTrainContext)


@dataclass
class YoloTrainRuntimeConfig:
    models_dir: str
    max_within_time: str
    tmp_folder: str
    model_suffix: str
    dt__model: DataTable
    dt__link: DataTable
    dt__training_status: DataTable
    model_other_primary_keys: List[str]
    model_id__name: str
    frozen_dataset_id__name: str
    ignore_errors_sample_sizes: bool
    training_launcher_config: Any = None
    sync_config: Optional[TrainingSyncConfig] = None
    resume_config: Optional[TrainingResumeConfig] = None
    extra_class_names_to_yaml_fields: List[str] = field(default_factory=list)

    @classmethod
    def from_kwargs(
        cls,
        kwargs: Dict[str, Any],
        *,
        model_table_key: str,
        link_table_key: str,
        model_other_primary_keys_key: str,
        model_id_key: str,
        frozen_dataset_id_key: str,
    ) -> "YoloTrainRuntimeConfig":
        return cls(
            models_dir=kwargs["models_dir"],
            max_within_time=kwargs["max_within_time"],
            tmp_folder=kwargs["tmp_folder"],
            model_suffix=kwargs["model_suffix"],
            dt__model=kwargs[model_table_key],
            dt__link=kwargs[link_table_key],
            dt__training_status=kwargs["dt__training_status"],
            model_other_primary_keys=kwargs[model_other_primary_keys_key],
            model_id__name=kwargs[model_id_key],
            frozen_dataset_id__name=kwargs[frozen_dataset_id_key],
            ignore_errors_sample_sizes=kwargs["ignore_errors_sample_sizes"],
            training_launcher_config=kwargs.get("training_launcher_config"),
            sync_config=kwargs.get("sync_config"),
            resume_config=kwargs.get("resume_config"),
            extra_class_names_to_yaml_fields=kwargs.get("extra_class_names_to_yaml_fields", []),
        )

    def build_context(
        self,
        context_cls: Type[YoloContextT],
        *,
        dt__frozen_dataset: DataTable,
        dt__train_config: DataTable,
        dt__class_names: DataTable,
        dt__resized_image_file: DataTable,
        dt__yolo_txt: DataTable,
        **extra_fields: Any,
    ) -> YoloContextT:
        return context_cls(
            models_dir=self.models_dir,
            max_within_time=self.max_within_time,
            tmp_folder=self.tmp_folder,
            model_suffix=self.model_suffix,
            dt__model=self.dt__model,
            dt__link=self.dt__link,
            dt__training_status=self.dt__training_status,
            dt__frozen_dataset=dt__frozen_dataset,
            dt__frozen_dataset__has__image_gt=None,
            dt__train_config=dt__train_config,
            model_other_primary_keys=self.model_other_primary_keys,
            model_id__name=self.model_id__name,
            frozen_dataset_id__name=self.frozen_dataset_id__name,
            dt__class_names=dt__class_names,
            dt__resized_image_file=dt__resized_image_file,
            dt__yolo_txt=dt__yolo_txt,
            ignore_errors_sample_sizes=self.ignore_errors_sample_sizes,
            extra_class_names_to_yaml_fields=self.extra_class_names_to_yaml_fields,
            training_launcher_config=self.training_launcher_config,
            sync_config=self.sync_config,
            resume_config=self.resume_config,
            **extra_fields,
        )


class YoloBaseAlgo(Algo):
    """
    Base strategy for YOLO-like algorithms (v5/v8 detect/segment).
    Subclasses must only provide:
      - train_config_id_col, train_params_col, frozen_created_at_col, images_count_col, model_row_prefix
      - type_name (e.g. "yolov5"/"yolov8")
      - TrainingConfigClass, DataYAMLClass, train_process_func
      - model_key ("weights" for v5, "model" for v8)
      - metrics_mAP_05_col / metrics_mAP_0595_col
      - threshold_mode: "best_threshold"
      - task (None | "detect" | "segment")   # used by v8 train_process
    """

    # --- subclass must set these ---
    type_name: str = "yolo"
    TrainingConfigClass: Any = None
    train_process_func: Optional[Callable[..., Any]] = None
    model_key: str = "model"  # v5 overrides to "weights"
    metrics_mAP_05_col: str = "metrics_mAP_0_5"
    metrics_mAP_0595_col: str = "metrics_mAP_0_5_to_0_95"
    threshold_mode: str = "best_threshold"
    task: Optional[str] = "detect"  # v5 uses None

    def check_accelerator(self, train_params: Dict[str, Any]) -> None:
        import torch

        if train_params.get("device") == "cpu":
            return

        if not torch.cuda.is_available():
            raise ValueError("CUDA not found.")

    # ---------- Common data preparation ----------
    def prepare_data(self, ctx: TrainContext, idx) -> YoloPreparedData:
        yctx = cast(YoloTrainContext, ctx)
        if yctx.dt__class_names is None or yctx.dt__resized_image_file is None or yctx.dt__yolo_txt is None:
            raise ValueError("YOLO context requires dt__class_names, dt__resized_image_file, dt__yolo_txt")
        df_fd = yctx.dt__frozen_dataset.get_data(idx)
        df_cls = yctx.dt__class_names.get_data(idx)
        df_img = yctx.dt__resized_image_file.get_data(idx)
        df_txt = yctx.dt__yolo_txt.get_data(idx)

        df_img = pd.merge(df_fd, df_img)
        df_txt = pd.merge(df_fd, df_txt)

        if len(df_img) != len(df_txt):
            raise ValueError("Images and yolo's txt lengts must be same")

        paths_train = df_txt[df_txt["subset_id"] == "train"]["filepath"].tolist()
        paths_val = df_txt[df_txt["subset_id"] == "val"]["filepath"].tolist()
        paths_test = df_txt[df_txt["subset_id"] == "test"]["filepath"].tolist()

        # Assert correct YOLO images locations
        filepaths_train = set([str(Pathy.fluid(p).parent) for p in paths_train])
        filepaths_val = set([str(Pathy.fluid(p).parent) for p in paths_val])
        assert len(filepaths_train) == 1 and len(filepaths_val) == 1
        p_train = Pathy.fluid(list(filepaths_train)[0])
        p_val = Pathy.fluid(list(filepaths_val)[0])
        assert p_train.name == "labels" and p_val.name == "labels"
        assert str(p_train.parent.parent) == str(p_val.parent.parent)

        data_src_path = str(Pathy.fluid(str(p_train.parent.parent)))
        total = len(paths_train) + len(paths_val) + len(paths_test)
        if self.images_count_col is None:
            raise ValueError("images_count_col must be set in YOLO algo")
        images_count = df_fd.iloc[0][self.images_count_col]
        if total != images_count and not (yctx.ignore_errors_sample_sizes or False):
            raise ValueError(f"Total images length {total} != {self.images_count_col}={images_count}")

        return YoloPreparedData(
            data_src_path=data_src_path,
            objects_count=len(paths_train) + len(paths_val),
            image_filepaths=df_img["filepath"].tolist(),
            yolo_txt_filepaths=df_txt["filepath"].tolist(),
            images_data_paths_train=paths_train,
            images_data_paths_val=paths_val,
            images_data_paths_test=paths_test,
            class_names=df_cls.iloc[0]["class_names"],
        )

    # ---------- Common model_id builder ----------
    def build_model_id(self, ctx: TrainContext, idx, train_params: Dict[str, Any]) -> str:
        date = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")
        prefix = (
            "__".join([str(idx.loc[0][k]) for k in ctx.model_other_primary_keys])
            if ctx.model_other_primary_keys
            else ""
        )
        batch_key = "batch_size" if self.model_key == "weights" else "batch"
        summary = build_yolo_train_config_summary(
            train_params,
            model_key=self.model_key,
            batch_key=batch_key,
        )
        config_id = build_train_config_id(train_params, summary=summary)
        core = f"{date}_{config_id}{ctx.model_suffix}"
        return f"{prefix + ('-' if prefix else '')}{core}"

    # ---------- Common training launcher (v8) ----------
    def _build_training_config(
        self, ctx: TrainContext, idx: IndexDF, model_id: str, train_params: Dict[str, Any], data: YoloPreparedData
    ):
        # Instantiate concrete TrainingConfig & DataYAML
        yctx = cast(YoloTrainContext, ctx)
        cfg = self.TrainingConfigClass(**train_params)
        cfg.tmp_folder = yctx.tmp_folder
        data_yaml = YoloDataYAMLConfig(
            path=data.data_src_path,
            train="train/images",
            val="val/images",
            test=("test/images" if data.images_data_paths_test and len(data.images_data_paths_test) > 0 else None),
            nc=len(data.class_names),
            names=data.class_names,
        )
        class_names_row = yctx.dt__class_names.get_data(idx).iloc[0]
        if "kpt_shape" in yctx.extra_class_names_to_yaml_fields:
            data_yaml.kpt_shape = class_names_row.get("kpt_shape")
        if "flip_idx" in yctx.extra_class_names_to_yaml_fields:
            data_yaml.flip_idx = class_names_row.get("flip_idx")
        cfg.data = data_yaml
        cfg.project = ctx.training_output_write_dir or str(yctx.models_dir)
        cfg.name = model_id
        cfg.persisted_project_dir = str(yctx.models_dir)
        return cfg

    def apply_resume_checkpoint(
        self,
        ctx: TrainContext,
        train_params: Dict[str, Any],
        checkpoint_path: Optional[str],
    ) -> Dict[str, Any]:
        params = dict(train_params)
        if checkpoint_path is None:
            return params
        params["initial_weights_path"] = checkpoint_path
        params["resume"] = True
        params["exist_ok"] = True
        params["save_period"] = max(1, int(params.get("save_period", -1)))
        return params

    def launch_training(
        self, ctx: TrainContext, idx: IndexDF, model_id: str, train_params: Dict[str, Any], data: PreparedData
    ) -> Any:
        """
        Default launcher for YOLOv8-style train_process:
        train_process(queue, cfg, objects_count, class_names, image_filepaths, yolo_txt_filepaths, sync_config, task)
        Subclasses can override for YOLOv5.
        """
        d = cast(YoloPreparedData, data)
        cfg = self._build_training_config(ctx, idx, model_id, train_params, d)
        train_proc = cast(Callable[..., Any], self.train_process_func)
        yctx = cast(YoloTrainContext, ctx)
        subprocess_sync_config = None if ctx.training_output_write_dir else yctx.sync_config
        launcher = build_training_launcher(ctx.training_launcher_config)
        return launcher.launch(
            TrainingLaunchRequest.from_path_maps(
                target=train_proc,
                args=(
                    cfg,
                    d.objects_count,
                    d.class_names,
                    d.image_filepaths,
                    d.yolo_txt_filepaths,
                    subprocess_sync_config,
                    self.task,
                ),
                cluster_suffix=model_id,
                inputs=(TrainingPathMap(d.data_src_path, "/workspace/datapipe_ml/input/data"),),
                outputs=(TrainingPathMap(str(yctx.models_dir), "/workspace/datapipe_ml/output/models"),),
            )
        )

    def run_dir_from_model_path(self, model_path: str) -> str:
        return str(Pathy.fluid(model_path).parent.parent)

    def collect_checkpoint_paths(
        self,
        raw_result: "YoloV5TrainModelResult | YoloV8TrainModelResult",
    ) -> List[str]:
        if raw_result.training_results is None:
            return []
        return sorted({str(item.model_path) for item in raw_result.training_results if item.model_path})

    @staticmethod
    def _input_size_from_params(train_params: Dict[str, Any]) -> Tuple[int, int]:
        imgsz = train_params.get("imgsz")
        if isinstance(imgsz, int):
            return (imgsz, imgsz)
        if isinstance(imgsz, (tuple, list)) and len(imgsz) == 2:
            return tuple(imgsz)  # type: ignore[return-value]
        raise ValueError("Cannot infer YOLO input size from training params.")

    def build_model_row(
        self,
        ctx: TrainContext,
        idx: IndexDF,
        model_id: str,
        best: Dict[str, Any],
        train_params: Dict[str, Any],
    ) -> pd.DataFrame:
        prefix = self.model_row_prefix
        row: dict[str, Any] = {k: idx.loc[0][k] for k in ctx.model_other_primary_keys}
        row[ctx.model_id__name] = model_id
        row[f"{prefix}__input_size"] = self._input_size_from_params(train_params)
        row[f"{prefix}__score_threshold"] = float(best.get("score_threshold", 0.45))
        row[f"{prefix}__model_path"] = best["model_path"]
        row[f"{prefix}__type"] = best.get("type_name", "model")
        row[f"{prefix}__class_names"] = best["class_names"]
        for column_name, metric_name in self.extra_model_metric_map.items():
            row[f"{prefix}__{column_name}"] = best.get("metrics", {}).get(metric_name)
        return pd.DataFrame([row])

    # ---------- Common "select best" ----------
    def select_best(self, raw_result: Any, idx: IndexDF) -> Dict[str, Any]:
        if raw_result.training_results is None:
            raise ValueError(f"Train failed: '{raw_result.traceback_logs}'")
        df = pd.DataFrame(raw_result.training_results)
        df["fitness"] = df.apply(
            lambda r: 0.1 * r[self.metrics_mAP_05_col] + 0.9 * r[self.metrics_mAP_0595_col], axis=1
        )
        best = df.iloc[[int(df["fitness"].argmax())]]

        if self.threshold_mode == "best_threshold":
            thr = float(best.iloc[0]["best_threshold"])
        else:
            thr = 0.45

        return dict(
            model_path=best.iloc[0]["model_path"],
            class_names=best.iloc[0]["class_names"],
            score_threshold=float(thr),
            type_name=self.type_name,
            metrics=best.iloc[0].to_dict(),
        )
