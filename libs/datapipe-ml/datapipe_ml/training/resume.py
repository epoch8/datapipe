from __future__ import annotations

from typing import Callable, Optional

from datapipe_ml.training.specs import TrainingResumeCheckpoint, TrainingResumeConfig
from datapipe_ml.training.sync import (
    TrainingCheckpointEntry,
    read_checkpoint_manifest,
    verify_manifest_checkpoint,
)


def _default_meets_min_epochs(item: TrainingCheckpointEntry, config: TrainingResumeConfig) -> bool:
    if item.epoch is not None:
        return item.epoch >= config.min_completed_epochs
    return config.min_completed_epochs <= 0


def read_manifest_verified_candidates(
    manifest_checkpoints: list[TrainingCheckpointEntry],
    config: TrainingResumeConfig,
    *,
    meets_min_epochs: Callable[[TrainingCheckpointEntry, TrainingResumeConfig], bool] = _default_meets_min_epochs,
) -> list[TrainingCheckpointEntry]:
    return [
        item
        for item in manifest_checkpoints
        if item.complete
        and verify_manifest_checkpoint(item)
        and meets_min_epochs(item, config)
    ]


def select_default_resume_checkpoint(
    *,
    manifest_path: Optional[str],
    config: Optional[TrainingResumeConfig],
) -> Optional[TrainingResumeCheckpoint]:
    if config is None or not config.continue_train_failed_models or manifest_path is None:
        return None
    manifest = read_checkpoint_manifest(manifest_path)
    if manifest is None:
        return None
    candidates = read_manifest_verified_candidates(manifest.checkpoints, config)
    if not candidates:
        return None
    with_epoch = [item for item in candidates if item.epoch is not None]
    if with_epoch:
        selected = max(with_epoch, key=lambda item: item.epoch or 0)
    else:
        selected = candidates[-1]
    return TrainingResumeCheckpoint(path=selected.path, epoch=selected.epoch)
