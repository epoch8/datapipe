from __future__ import annotations

import json
import logging
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, List, Optional, TypeVar

from datapipe_ml.frameworks.yolo.artifacts import (
    yolo_load_data_config,
    yolo_write_data_yaml_if_needed,
)
from datapipe_ml.training.specs import TrainingSyncConfig
from datapipe_ml.training.sync import PeriodicTrainingSync

logger = logging.getLogger("datapipe.ml.yolo.train_session")

T = TypeVar("T")


def yolo_resolve_class_names(training_config, class_names: List[str]) -> List[str]:
    if class_names:
        return class_names
    if isinstance(training_config.data, str):
        try:
            cfg = yolo_load_data_config(training_config.data)
            return list(cfg.names)
        except Exception:
            return []
    return []


def yolo_make_training_sync(
    *,
    training_config,
    sync_config: Optional[TrainingSyncConfig],
    tmp_dir_project,
    src_project_path,
) -> Optional[PeriodicTrainingSync]:
    if sync_config is None or not sync_config.enabled:
        return None
    return PeriodicTrainingSync(
        src=str(tmp_dir_project or training_config.project),
        dst=str(src_project_path or training_config.project),
        config=sync_config,
        model_id=training_config.name,
    )


def yolo_write_exp_metadata(exp_folder: Path, tmp_yaml_path: Optional[Path], class_names: List[str]) -> None:
    if tmp_yaml_path and tmp_yaml_path.exists():
        shutil.copy(str(tmp_yaml_path), exp_folder / "training_data.yaml")
    with open(exp_folder / "class_names.json", "w") as out:
        json.dump(class_names, out, indent=4, ensure_ascii=False)


def yolo_cleanup_temp_files(*paths: Optional[Path]) -> None:
    for temp_path in paths:
        if temp_path and temp_path.exists():
            temp_path.unlink(missing_ok=True)


@dataclass
class YoloTrainSession:
    training_config: object
    sync_config: Optional[TrainingSyncConfig]
    src_project_path: Optional[str]
    tmp_dir_project: Optional[str]
    extra_temp_paths: tuple[Optional[Path], ...] = ()

    def run(self, launch_fn: Callable[[List[str], Optional[Path]], T]) -> tuple[T, List[str]]:
        class_names, tmp_yaml_path = yolo_write_data_yaml_if_needed(self.training_config)
        try:
            class_names = yolo_resolve_class_names(self.training_config, class_names)
            output_sync = yolo_make_training_sync(
                training_config=self.training_config,
                sync_config=self.sync_config,
                tmp_dir_project=self.tmp_dir_project,
                src_project_path=self.src_project_path,
            )
            if output_sync is None:
                result = launch_fn(class_names, tmp_yaml_path)
            else:
                with output_sync:
                    result = launch_fn(class_names, tmp_yaml_path)
            return result, class_names
        finally:
            yolo_cleanup_temp_files(tmp_yaml_path, *self.extra_temp_paths)
