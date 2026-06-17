from __future__ import annotations

import contextlib
import os
import tempfile
import time
from pathlib import Path, PurePosixPath
from typing import Iterator, Union

import fsspec

PathLike = Union[str, Path]


def _fsync_file(path: Path) -> None:
    with open(path, "rb") as handle:
        os.fsync(handle.fileno())


def _fsync_parent_dir(path: Path) -> None:
    dir_fd = os.open(str(path.parent), os.O_RDONLY)
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)


@contextlib.contextmanager
def atomic_write_local(path: PathLike) -> Iterator[Path]:
    """Write via a sibling temp file and replace the destination on success."""
    final_path = Path(path)
    if final_path.exists() and final_path.is_dir():
        raise FileExistsError(f"Cannot atomically replace directory: {final_path}")
    final_path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(
        prefix=f".{final_path.name}.",
        suffix=".tmp",
        dir=final_path.parent,
    )
    os.close(fd)
    tmp_path = Path(tmp_name)
    try:
        yield tmp_path
        if not tmp_path.is_file():
            raise FileNotFoundError(f"Atomic write produced no file: {tmp_path}")
        _fsync_file(tmp_path)
        tmp_path.replace(final_path)
        _fsync_parent_dir(final_path)
    finally:
        if tmp_path.exists():
            tmp_path.unlink(missing_ok=True)


def publish_file_atomically(src: str, dst_url: str, *, label: str = "file") -> None:
    """Copy ``src`` to ``dst_url`` through a temporary destination path."""
    from datapipe_ml.core.files import copy_url_to_url

    dst_fs, dst_path = fsspec.core.url_to_fs(dst_url)
    parent = str(PurePosixPath(dst_path).parent)
    dst_fs.makedirs(parent, exist_ok=True)
    tmp_url = f"{dst_url}.tmp.{os.getpid()}.{time.time_ns()}"
    tmp_fs, tmp_path = fsspec.core.url_to_fs(tmp_url)
    try:
        copy_url_to_url(src, tmp_url, label=label, concurrency=1)
        _replace_url_atomically(dst_fs, tmp_path, dst_path)
    finally:
        if tmp_fs.exists(tmp_path):
            tmp_fs.rm(tmp_path)


def _replace_url_atomically(dst_fs: fsspec.AbstractFileSystem, tmp_path: str, dst_path: str) -> None:
    if hasattr(dst_fs, "mv"):
        dst_fs.mv(tmp_path, dst_path)
        return
    with fsspec.open(tmp_path, "rb") as src_file:
        with fsspec.open(dst_path, "wb") as dst_file:
            dst_file.write(src_file.read())


def write_bytes_atomically(dst_url: str, payload: bytes, *, label: str = "file") -> None:
    """Write ``payload`` to ``dst_url`` through a temporary destination path."""
    del label
    dst_fs, dst_path = fsspec.core.url_to_fs(dst_url)
    parent = str(PurePosixPath(dst_path).parent)
    dst_fs.makedirs(parent, exist_ok=True)
    tmp_url = f"{dst_url}.tmp.{os.getpid()}.{time.time_ns()}"
    tmp_fs, tmp_path = fsspec.core.url_to_fs(tmp_url)
    try:
        with fsspec.open(tmp_url, "wb") as out:
            out.write(payload)
        _replace_url_atomically(dst_fs, tmp_path, dst_path)
    finally:
        if tmp_fs.exists(tmp_path):
            tmp_fs.rm(tmp_path)
