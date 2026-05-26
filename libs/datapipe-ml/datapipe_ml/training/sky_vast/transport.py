from __future__ import annotations

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
    project_root = Path(__file__).resolve().parents[3]
    with tempfile.NamedTemporaryFile(suffix=".tar.gz") as archive:
        with tarfile.open(archive.name, "w:gz") as tar:
            for name in ("pyproject.toml", "README.md", "datapipe_ml"):
                local_path = project_root / name
                if local_path.exists():
                    tar.add(local_path, arcname=name)
        with open(archive.name, "rb") as in_file:
            with sshfs.open(dst, "wb") as out_file:
                out_file.write(in_file.read())
