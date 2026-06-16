from __future__ import annotations

import io
import logging
import zipfile

import fsspec

logger = logging.getLogger(__name__)


def is_zip_checkpoint_loadable(path: str) -> bool:
    """Best-effort check that a .pt/.keras checkpoint archive is readable."""
    try:
        with fsspec.open(path, "rb") as src:
            payload = src.read()
    except FileNotFoundError:
        return False
    except OSError:
        logger.warning("Could not read checkpoint for load verify: %s", path)
        return False
    buffer = io.BytesIO(payload)
    if not zipfile.is_zipfile(buffer):
        return False
    buffer.seek(0)
    try:
        with zipfile.ZipFile(buffer) as archive:
            return archive.testzip() is None
    except (zipfile.BadZipFile, OSError, ValueError):
        return False
