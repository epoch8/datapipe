from __future__ import annotations

import logging
from importlib.metadata import entry_points
from typing import Any, Callable

logger = logging.getLogger(__name__)

ObservabilityDbExtension = Callable[..., None]
V1Alpha3Extension = Callable[..., None]


def _load_entry_points(group: str) -> list[Any]:
    try:
        return list(entry_points(group=group))
    except TypeError:
        return list(entry_points().select(group=group))


def run_observability_db_extensions(**kwargs: Any) -> None:
    for ep in _load_entry_points("datapipe.observability_db"):
        try:
            ep.load()(**kwargs)
        except Exception:
            logger.exception("Failed observability DB extension: %s", ep.name)


def register_v1alpha3_extensions(**kwargs: Any) -> None:
    for ep in _load_entry_points("datapipe.v1alpha3_extensions"):
        try:
            ep.load()(**kwargs)
        except Exception:
            logger.exception("Failed v1alpha3 extension: %s", ep.name)
