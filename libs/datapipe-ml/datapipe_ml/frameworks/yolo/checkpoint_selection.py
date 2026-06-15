from __future__ import annotations

from pathlib import PurePosixPath
from typing import Optional

from datapipe_ml.training.resume import read_manifest_verified_candidates
from datapipe_ml.training.specs import TrainingResumeCheckpoint, TrainingResumeConfig
from datapipe_ml.training.sync import TrainingCheckpointEntry, read_checkpoint_manifest


def _checkpoint_basename(path: str) -> str:
    return PurePosixPath(path).name


def _yolo_meets_min_epochs(item: TrainingCheckpointEntry, config: TrainingResumeConfig) -> bool:
    if item.epoch is not None:
        return item.epoch >= config.min_completed_epochs
    if _checkpoint_basename(item.path) in {"last.pt", "best.pt"}:
        return True
    return config.min_completed_epochs <= 0


def select_yolo_resume_checkpoint(
    *,
    manifest_path: Optional[str],
    config: Optional[TrainingResumeConfig],
) -> Optional[TrainingResumeCheckpoint]:
    if config is None or not config.continue_train_failed_models or manifest_path is None:
        return None
    manifest = read_checkpoint_manifest(manifest_path)
    if manifest is None:
        return None
    candidates = read_manifest_verified_candidates(
        manifest.checkpoints,
        config,
        meets_min_epochs=_yolo_meets_min_epochs,
    )
    if not candidates:
        return None
    if config.checkpoint == "last":
        last_candidates = [item for item in candidates if _checkpoint_basename(item.path) == "last.pt"]
        if not last_candidates:
            return None
        selected = last_candidates[-1]
    elif config.checkpoint == "best":
        best_candidates = [item for item in candidates if _checkpoint_basename(item.path) == "best.pt"]
        if not best_candidates:
            return None
        selected = best_candidates[-1]
    else:
        selected = candidates[-1]
    return TrainingResumeCheckpoint(path=selected.path, epoch=selected.epoch)
