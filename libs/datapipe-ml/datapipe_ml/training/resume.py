from __future__ import annotations

from typing import Optional

from datapipe_ml.training.specs import TrainingResumeCheckpoint, TrainingResumeConfig
from datapipe_ml.training.sync import read_checkpoint_manifest, verify_manifest_checkpoint


def select_resume_checkpoint(
    *,
    manifest_path: Optional[str],
    config: Optional[TrainingResumeConfig],
) -> Optional[TrainingResumeCheckpoint]:
    if config is None or not config.continue_train_failed_models or manifest_path is None:
        return None
    manifest = read_checkpoint_manifest(manifest_path)
    if manifest is None:
        return None
    candidates = [
        item
        for item in manifest.checkpoints
        if item.complete
        and verify_manifest_checkpoint(item)
        and (
            (item.epoch is not None and item.epoch >= config.min_completed_epochs)
            or (item.epoch is None and config.min_completed_epochs <= 0)
        )
    ]
    if not candidates:
        return None
    epoch_candidates = [item for item in candidates if item.epoch is not None]
    if config.checkpoint in {"last", "latest_epoch"} and epoch_candidates:
        selected = max(epoch_candidates, key=lambda item: int(item.epoch or -1))
    elif config.checkpoint == "best":
        best_candidates = [item for item in candidates if item.path.endswith("/best.pt") or item.path.endswith("best.pt")]
        selected = best_candidates[-1] if best_candidates else candidates[-1]
    else:
        selected = candidates[-1]
    return TrainingResumeCheckpoint(path=selected.path, epoch=selected.epoch)
