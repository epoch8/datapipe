from __future__ import annotations

import logging
from importlib.metadata import entry_points
from typing import Optional

logger = logging.getLogger(__name__)

_UI_STATIC_PREFERENCE = ("datapipe_ui_ml", "datapipe_ui")


def resolve_frontend_dir() -> Optional[str]:
    """Load a registered datapipe.ui_static entry point (ML SPA preferred)."""
    available = {ep.name: ep for ep in entry_points(group="datapipe.ui_static")}
    for name in _UI_STATIC_PREFERENCE:
        ep = available.get(name)
        if ep is None:
            continue
        try:
            return ep.load()()
        except Exception:
            logger.exception("Failed to load UI static entry point %s", ep.name)

    for ep in available.values():
        if ep.name in _UI_STATIC_PREFERENCE:
            continue
        try:
            return ep.load()()
        except Exception:
            logger.exception("Failed to load UI static entry point %s", ep.name)
    return None
