from __future__ import annotations

import logging
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)

_training_curve_hook: Optional[Callable[..., None]] = None


def register_training_curve_hook(fn: Callable[..., None]) -> None:
    global _training_curve_hook
    _training_curve_hook = fn


def maybe_publish_training_curves(**kwargs: Any) -> None:
    if _training_curve_hook is None:
        return
    try:
        _training_curve_hook(**kwargs)
    except Exception:
        logger.exception("Training curve publish hook failed")
