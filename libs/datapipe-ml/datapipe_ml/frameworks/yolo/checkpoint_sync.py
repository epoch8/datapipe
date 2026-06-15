from __future__ import annotations

from pathlib import Path, PurePosixPath
from typing import Optional

import fsspec
from pathy import Pathy


def _relative_posix_path(path: str, base: str) -> str:
    return str(PurePosixPath(path).relative_to(PurePosixPath(base)))


def discover_checkpoint_paths_in_run_dir(run_dir: str) -> list[str]:
    fs, stripped_run_dir = fsspec.core.url_to_fs(run_dir)
    if not fs.exists(stripped_run_dir):
        return []
    paths: list[str] = []
    for path in fs.find(stripped_run_dir):
        if not fs.isfile(path):
            continue
        name = PurePosixPath(path).name
        if (name.startswith("epoch") and name.endswith(".pt")) or name in {"last.pt", "best.pt"}:
            paths.append(str(Pathy.fluid(run_dir) / _relative_posix_path(path, stripped_run_dir)))
    return sorted(paths)


def infer_epoch_from_checkpoint_path(path: str) -> Optional[int]:
    name = PurePosixPath(path).name
    if name.startswith("epoch") and name.endswith(".pt"):
        raw = name[len("epoch") : -len(".pt")]
        return int(raw) if raw.isdigit() else None
    if name in {"last.pt", "best.pt"}:
        return _infer_epoch_from_pt_checkpoint(path)
    return None


def _infer_epoch_from_pt_checkpoint(path: str) -> Optional[int]:
    try:
        import torch
    except ImportError:
        return None
    try:
        checkpoint = torch.load(path, map_location="cpu", weights_only=False)
    except Exception:
        return None
    if not isinstance(checkpoint, dict):
        return None
    raw_epoch = checkpoint.get("epoch")
    if raw_epoch is None:
        return None
    epoch = int(raw_epoch)
    # YOLO checkpoints store a zero-based epoch index; report completed epoch count.
    return epoch + 1


def max_completed_epoch_from_run_dir(run_dir: str | Path) -> Optional[int]:
    weights_dir = Path(run_dir) / "weights"
    if not weights_dir.is_dir():
        return None
    max_epoch: Optional[int] = None
    for checkpoint_path in weights_dir.glob("epoch*.pt"):
        parsed = infer_epoch_from_checkpoint_path(str(checkpoint_path))
        if parsed is None:
            continue
        max_epoch = parsed if max_epoch is None else max(max_epoch, parsed)
    for alias_name in ("last.pt", "best.pt"):
        alias_path = weights_dir / alias_name
        if not alias_path.exists():
            continue
        parsed = _infer_epoch_from_pt_checkpoint(str(alias_path))
        if parsed is None:
            continue
        max_epoch = parsed if max_epoch is None else max(max_epoch, parsed)
    return max_epoch
