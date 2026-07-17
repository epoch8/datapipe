from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import List, Optional

from datapipe_ml.frameworks.tensorflow.checkpoint_sync import is_tf_last_checkpoint_path
from datapipe_ml.training.checkpoint_verify import is_zip_checkpoint_loadable
from datapipe_ml.training.resume import read_manifest_verified_candidates, select_first_loadable_checkpoint
from datapipe_ml.training.specs import TrainingResumeCheckpoint, TrainingResumeConfig

is_tf_checkpoint_loadable = is_zip_checkpoint_loadable

logger = logging.getLogger(__name__)

_VAL_F1_SCORE_CHECKPOINT_RE = re.compile(r"val_f1_score_([\d.]+)\.keras$")


def select_best_classification_checkpoint(paths: List[str]) -> Optional[str]:
    best_path: Optional[str] = None
    best_score = float("-inf")
    for path in paths:
        if is_tf_last_checkpoint_path(path):
            continue
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


def _order_tf_last_candidates(candidates: list) -> list:
    from datapipe_ml.training.sync import TrainingCheckpointEntry

    typed = [item for item in candidates if isinstance(item, TrainingCheckpointEntry)]
    last = [item for item in typed if is_tf_last_checkpoint_path(item.path)]
    rest = sorted(
        [item for item in typed if not is_tf_last_checkpoint_path(item.path)],
        key=lambda item: item.epoch or 0,
        reverse=True,
    )
    return last + rest


def _order_tf_best_candidates(candidates: list) -> list:
    from datapipe_ml.training.sync import TrainingCheckpointEntry

    typed = [item for item in candidates if isinstance(item, TrainingCheckpointEntry)]
    best_paths = {
        path
        for path in [select_best_classification_checkpoint([item.path for item in typed])]
        if path is not None
    }
    best = [item for item in typed if item.path in best_paths]
    rest = sorted(
        [item for item in typed if item.path not in best_paths],
        key=lambda item: item.epoch or 0,
        reverse=True,
    )
    return best + rest


def select_tf_resume_checkpoint(
    *,
    manifest_path: Optional[str],
    config: Optional[TrainingResumeConfig],
) -> Optional[TrainingResumeCheckpoint]:
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
        ordered = _order_tf_best_candidates(candidates)
    elif config.checkpoint == "last":
        ordered = _order_tf_last_candidates(candidates)
    else:
        ordered = sorted(candidates, key=lambda item: item.epoch or 0, reverse=True)
    selected = select_first_loadable_checkpoint(ordered, is_loadable=is_tf_checkpoint_loadable)
    if selected is None:
        return None
    return TrainingResumeCheckpoint(path=selected.path, epoch=selected.epoch)

