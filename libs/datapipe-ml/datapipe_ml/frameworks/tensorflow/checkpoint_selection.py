from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import List, Optional

from datapipe_ml.training.specs import TrainingResumeCheckpoint, TrainingResumeConfig

logger = logging.getLogger(__name__)

_VAL_F1_SCORE_CHECKPOINT_RE = re.compile(r"val_f1_score_([\d.]+)\.keras$")


def select_best_classification_checkpoint(paths: List[str]) -> Optional[str]:
    best_path: Optional[str] = None
    best_score = float("-inf")
    for path in paths:
        match = _VAL_F1_SCORE_CHECKPOINT_RE.search(Path(path).name)
        if match is None:
            continue
        score = float(match.group(1))
        if score > best_score:
            best_score = score
            best_path = path
    if best_path is not None:
        return best_path
    if not paths:
        return None
    logger.warning(
        "Could not parse val_f1_score from checkpoint filenames; falling back to lexicographic last: %s",
        paths,
    )
    return sorted(paths)[-1]


def select_tf_resume_checkpoint(
    *,
    manifest_path: Optional[str],
    config: Optional[TrainingResumeConfig],
) -> Optional[TrainingResumeCheckpoint]:
    from datapipe_ml.training.resume import read_manifest_verified_candidates
    from datapipe_ml.training.sync import read_checkpoint_manifest

    if config is None or not config.continue_train_failed_models or manifest_path is None:
        return None
    manifest = read_checkpoint_manifest(manifest_path)
    if manifest is None:
        return None
    candidates = read_manifest_verified_candidates(manifest.checkpoints, config)
    if not candidates:
        return None
    if config.checkpoint == "best":
        selected_path = select_best_classification_checkpoint([item.path for item in candidates])
        if selected_path is None:
            return None
        matching = [item for item in candidates if item.path == selected_path]
        selected = matching[-1]
    else:
        with_epoch = [item for item in candidates if item.epoch is not None]
        selected = max(with_epoch, key=lambda item: item.epoch or 0) if with_epoch else candidates[-1]
    return TrainingResumeCheckpoint(path=selected.path, epoch=selected.epoch)

