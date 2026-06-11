from __future__ import annotations

import importlib.util
import logging
import signal
import tarfile
import tempfile
import time
from pathlib import Path
from types import FrameType
from typing import Any, Callable, Optional

from fsspec import AbstractFileSystem

from datapipe_ml.core import files as file_transport

logger = logging.getLogger("datapipe.ml.sky_vast")


def _package_root(import_name: str, *, project_file: str = "pyproject.toml") -> Path:
    spec = importlib.util.find_spec(import_name)
    if spec is None or spec.submodule_search_locations is None:
        raise RuntimeError(f"Could not find package {import_name!r} to copy Sky/Vast source")
    path = Path(next(iter(spec.submodule_search_locations))).resolve()
    for parent in (path.parent, *path.parents):
        if (parent / project_file).exists():
            return parent
    raise RuntimeError(f"Could not find {project_file} for package {import_name!r}")


def copy_tree_between_fs(
    src_fs: AbstractFileSystem,
    src: str,
    dst_fs: AbstractFileSystem,
    dst: str,
    *,
    concurrency: int,
) -> None:
    file_transport.copy_tree_between_fs(src_fs, src, dst_fs, dst, concurrency=concurrency)


def copy_local_or_cloud_to_fs(
    src: str,
    dst_fs: AbstractFileSystem,
    dst: str,
    *,
    label: str,
    concurrency: int,
) -> None:
    file_transport.copy_url_to_fs(src, dst_fs, dst, label=label, concurrency=concurrency)


def copy_fs_to_local_or_cloud(
    src_fs: AbstractFileSystem,
    src: str,
    dst: str,
    *,
    label: str,
    concurrency: int,
) -> None:
    file_transport.copy_fs_to_url(src_fs, src, dst, label=label, concurrency=concurrency)


class Timeout:
    def __init__(self, seconds: int, label: str):
        self.seconds = seconds
        self.label = label
        self._previous_handler: Callable[[int, FrameType | None], Any] | int | None = None

    def __enter__(self):
        if self.seconds > 0 and hasattr(signal, "SIGALRM"):
            self._previous_handler = signal.signal(signal.SIGALRM, self._raise_timeout)
            signal.alarm(self.seconds)
        return self

    def __exit__(self, exc_type, exc, tb):
        if self.seconds > 0 and hasattr(signal, "SIGALRM"):
            signal.alarm(0)
            if self._previous_handler is not None:
                signal.signal(signal.SIGALRM, self._previous_handler)
        return False

    def _raise_timeout(self, signum: int, frame: FrameType | None) -> None:
        raise TimeoutError(f"Timed out while {self.label} after {self.seconds}s")


def retry(fn: Callable[[], Any], *, attempts: int, sleep_s: int, label: str) -> Any:
    last_exc: Optional[BaseException] = None
    for attempt in range(attempts + 1):
        try:
            return fn()
        except BaseException as exc:
            last_exc = exc
            if attempt >= attempts:
                break
            logger.info("[%s] retry %s/%s: %s", label, attempt + 1, attempts, exc)
            time.sleep(sleep_s)
    assert last_exc is not None
    raise last_exc


def copy_project_source_to_remote(sshfs: AbstractFileSystem, dst: str) -> None:
    datapipe_core_root = _package_root("datapipe")
    datapipe_ml_root = _package_root("datapipe_ml")
    datapipe_core_readme = (datapipe_core_root / "../../README.md").resolve()
    source_paths = (
        (datapipe_core_readme, "README.md"),
        (datapipe_core_root / "pyproject.toml", "datapipe-core/pyproject.toml"),
        (datapipe_core_root / "datapipe", "datapipe-core/datapipe"),
        (datapipe_ml_root / "pyproject.toml", "datapipe-ml/pyproject.toml"),
        (datapipe_ml_root / "README.md", "datapipe-ml/README.md"),
        (datapipe_ml_root / "datapipe_ml", "datapipe-ml/datapipe_ml"),
    )
    with tempfile.NamedTemporaryFile(suffix=".tar.gz") as archive:
        with tarfile.open(archive.name, "w:gz") as tar:
            for local_path, archive_name in source_paths:
                if local_path.exists():
                    tar.add(local_path, arcname=archive_name)
        with open(archive.name, "rb") as in_file:
            with sshfs.open(dst, "wb") as out_file:
                out_file.write(in_file.read())
