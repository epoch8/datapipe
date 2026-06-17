from __future__ import annotations

import logging
from typing import Callable, Optional

from datapipe_ml.training import checkpoint_verify
from datapipe_ml.training.specs import TrainingResumeCheckpoint, TrainingResumeConfig
from datapipe_ml.training.sync import (
    TrainingCheckpointEntry,
    read_checkpoint_manifest,
    verify_manifest_checkpoint,
)

logger = logging.getLogger(__name__)


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


def select_first_loadable_checkpoint(
    ordered_candidates: list[TrainingCheckpointEntry],
    *,
    is_loadable: Callable[[str], bool] | None = None,
) -> Optional[TrainingCheckpointEntry]:
    loadable = is_loadable or checkpoint_verify.is_zip_checkpoint_loadable
    for item in ordered_candidates:
        if not loadable(item.path):
            logger.warning("Resume checkpoint is not loadable, trying next candidate: %s", item.path)
            continue
        return item
    return None


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
        ordered = sorted(with_epoch, key=lambda item: item.epoch or 0, reverse=True)
    else:
        ordered = list(candidates)
    selected = select_first_loadable_checkpoint(ordered)
    if selected is None:
        return None
    return TrainingResumeCheckpoint(path=selected.path, epoch=selected.epoch)
