from __future__ import annotations

import contextlib
import logging
from pathlib import Path
from typing import Any, Iterator, Optional

from datapipe_ml.core.atomic_io import atomic_write_local

logger = logging.getLogger(__name__)


def _checkpoint_path_from_torch_save_target(target: Any) -> Optional[Path]:
    if isinstance(target, (str, Path)):
        return Path(target)
    target_name = getattr(target, "name", None)
    if isinstance(target_name, str) and target_name:
        return Path(target_name)
    return None


def _is_torch_checkpoint_path(path: Path) -> bool:
    return path.suffix == ".pt"


@contextlib.contextmanager
def atomic_yolo_checkpoint_io() -> Iterator[None]:
    """Make YOLO checkpoint writes atomic for the duration of training.

    - YOLOv5 saves with ``torch.save(..., path)``.
    - Ultralytics saves serialized bytes via ``Path.write_bytes(...)``.
    """
    try:
        import torch
    except ImportError:
        logger.debug("torch is not installed; atomic YOLO checkpoint IO disabled")
        yield
        return

    original_torch_save = torch.save
    original_path_write_bytes = Path.write_bytes

    def atomic_torch_save(obj: Any, target: Any, *args: Any, **kwargs: Any) -> None:
        checkpoint_path = _checkpoint_path_from_torch_save_target(target)
        if checkpoint_path is None or not _is_torch_checkpoint_path(checkpoint_path):
            original_torch_save(obj, target, *args, **kwargs)
            return
        with atomic_write_local(checkpoint_path) as tmp_path:
            original_torch_save(obj, tmp_path, *args, **kwargs)

    def atomic_path_write_bytes(self: Path, data: bytes) -> int:
        if _is_torch_checkpoint_path(self):
            with atomic_write_local(self) as tmp_path:
                return original_path_write_bytes(tmp_path, data)
        return original_path_write_bytes(self, data)

    torch.save = atomic_torch_save  # type: ignore[method-assign]
    Path.write_bytes = atomic_path_write_bytes  # type: ignore[method-assign]
    try:
        yield
    finally:
        torch.save = original_torch_save  # type: ignore[method-assign]
        Path.write_bytes = original_path_write_bytes  # type: ignore[method-assign]
