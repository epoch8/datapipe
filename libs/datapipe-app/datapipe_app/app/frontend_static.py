from __future__ import annotations

import logging
from importlib.metadata import entry_points
from typing import Optional

logger = logging.getLogger(__name__)


def resolve_frontend_dir() -> Optional[str]:
    """Load the first registered datapipe.ui_static entry point."""
    for ep in entry_points(group="datapipe.ui_static"):
        try:
            return ep.load()()
        except Exception:
            logger.exception("Failed to load UI static entry point %s", ep.name)
    return None
