from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import List, Optional

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
