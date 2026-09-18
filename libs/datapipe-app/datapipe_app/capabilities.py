"""Discover capability contributions from external addon libraries.

Entry-point group: ``datapipe.capabilities``.

Each entry point should resolve to either:
- an ``AddonCapability`` instance,
- a mapping validated as ``AddonCapability``,
- or a zero-arg callable returning one of the above.

The core API does not interpret addon feature keys (including any ML-related
ones); addons own their own schemas inside ``features``.
"""

from __future__ import annotations

import logging
from importlib.metadata import entry_points
from typing import Any, Iterable, List, Optional, Sequence

from datapipe_app.models import AddonCapability

logger = logging.getLogger(__name__)

CAPABILITIES_ENTRY_POINT_GROUP = "datapipe.capabilities"


def _load_entry_points(group: str) -> list[Any]:
    try:
        return list(entry_points(group=group))
    except TypeError:
        return list(entry_points().select(group=group))


def _normalize_addon(raw: Any, *, fallback_name: str) -> AddonCapability:
    if isinstance(raw, AddonCapability):
        return raw
    if callable(raw):
        return _normalize_addon(raw(), fallback_name=fallback_name)
    if isinstance(raw, dict):
        payload = dict(raw)
        payload.setdefault("name", fallback_name)
        return AddonCapability.model_validate(payload)
    raise TypeError(
        f"Capability provider {fallback_name!r} returned unsupported type {type(raw)!r}"
    )


def collect_addon_capabilities(
    *,
    extra: Optional[Sequence[AddonCapability]] = None,
    entry_point_group: str = CAPABILITIES_ENTRY_POINT_GROUP,
) -> List[AddonCapability]:
    """Load addon capabilities from entry points, then append ``extra``."""
    addons: List[AddonCapability] = []
    for ep in _load_entry_points(entry_point_group):
        try:
            addons.append(_normalize_addon(ep.load(), fallback_name=ep.name))
        except Exception:
            logger.exception("Failed to load capability addon: %s", ep.name)

    if extra:
        addons.extend(extra)
    return addons


def merge_addon_capabilities(
    *groups: Iterable[AddonCapability],
) -> List[AddonCapability]:
    """Stable de-dupe by addon name (later groups win)."""
    by_name: dict[str, AddonCapability] = {}
    for group in groups:
        for addon in group:
            by_name[addon.name] = addon
    return list(by_name.values())
