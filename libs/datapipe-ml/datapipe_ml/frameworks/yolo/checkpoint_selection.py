from __future__ import annotations

from pathlib import PurePosixPath
from typing import Optional

from datapipe_ml.training.checkpoint_verify import is_zip_checkpoint_loadable
from datapipe_ml.training.resume import read_manifest_verified_candidates, select_first_loadable_checkpoint
from datapipe_ml.training.specs import TrainingResumeCheckpoint, TrainingResumeConfig
from datapipe_ml.training.sync import TrainingCheckpointEntry, read_checkpoint_manifest

is_yolo_checkpoint_loadable = is_zip_checkpoint_loadable


def _checkpoint_basename(path: str) -> str:
    return PurePosixPath(path).name


def _yolo_meets_min_epochs(item: TrainingCheckpointEntry, config: TrainingResumeConfig) -> bool:
    if item.epoch is not None:
        return item.epoch >= config.min_completed_epochs
    if _checkpoint_basename(item.path) in {"last.pt", "best.pt"}:
        return True
    return config.min_completed_epochs <= 0


def _order_yolo_last_candidates(candidates: list[TrainingCheckpointEntry]) -> list[TrainingCheckpointEntry]:
    last = [item for item in candidates if _checkpoint_basename(item.path) == "last.pt"]
    epochs = sorted(
        [
            item
            for item in candidates
            if _checkpoint_basename(item.path) != "last.pt" and _checkpoint_basename(item.path) != "best.pt"
        ],
        key=lambda item: item.epoch or 0,
        reverse=True,
    )
    best = [item for item in candidates if _checkpoint_basename(item.path) == "best.pt"]
    return last + epochs + best


def _order_yolo_best_candidates(candidates: list[TrainingCheckpointEntry]) -> list[TrainingCheckpointEntry]:
    best = [item for item in candidates if _checkpoint_basename(item.path) == "best.pt"]
    rest = sorted(
        [item for item in candidates if _checkpoint_basename(item.path) != "best.pt"],
        key=lambda item: item.epoch or 0,
        reverse=True,
    )
    return best + rest


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
        ordered = _order_yolo_last_candidates(candidates)
    elif config.checkpoint == "best":
        ordered = _order_yolo_best_candidates(candidates)
    else:
        ordered = sorted(candidates, key=lambda item: item.epoch or 0, reverse=True)
    selected = select_first_loadable_checkpoint(ordered, is_loadable=is_yolo_checkpoint_loadable)
    if selected is None:
        return None
    return TrainingResumeCheckpoint(path=selected.path, epoch=selected.epoch)
