from __future__ import annotations

import json
import logging
import os
import threading
import time
from dataclasses import asdict, dataclass
from pathlib import PurePosixPath
from types import TracebackType
from typing import Callable, Iterable, Optional, Type

import fsspec
from pathy import Pathy

from datapipe_ml.training.specs import TrainingSyncConfig

MANIFEST_FILENAME = "datapipe_ml_training_sync.json"
logger = logging.getLogger("datapipe.ml.training.sync")


@dataclass
class TrainingCheckpointEntry:
    path: str
    epoch: Optional[int]
    size: int
    mtime: Optional[float]
    complete: bool = True


@dataclass
class TrainingCheckpointManifest:
    run_id: str
    model_id: str
    run_dir: str
    checkpoints: list[TrainingCheckpointEntry]


def _relative_posix_path(path: str, base: str) -> str:
    return str(PurePosixPath(path).relative_to(PurePosixPath(base)))


def _storage_options(url: str) -> dict:
    protocol, _path = fsspec.core.split_protocol(url)
    if protocol == "s3" and os.environ.get("S3_ENDPOINT_URL"):
        return {"client_kwargs": {"endpoint_url": os.environ["S3_ENDPOINT_URL"]}}
    return {}


def manifest_path_for_run(run_dir: str) -> str:
    return str(Pathy.fluid(run_dir) / MANIFEST_FILENAME)


def _stat_url(url: str) -> tuple[int, Optional[float]]:
    fs, path = fsspec.core.url_to_fs(url, **_storage_options(url))
    info = fs.info(path)
    size = int(info.get("size") or 0)
    mtime_raw = info.get("mtime") or info.get("LastModified")
    try:
        mtime = float(mtime_raw) if mtime_raw is not None else None
    except (TypeError, ValueError):
        mtime = None
    return size, mtime


def _stable_stat(url: str) -> tuple[int, Optional[float]]:
    first = _stat_url(url)
    # A short second read catches the common case where a mutable checkpoint is
    # being overwritten while we are publishing the manifest.
    time.sleep(0.01)
    second = _stat_url(url)
    if first != second:
        raise RuntimeError(f"Checkpoint changed while being inspected: {url}")
    return second


def infer_epoch_from_path(path: str) -> Optional[int]:
    name = PurePosixPath(path).name
    if name.startswith("epoch") and name.endswith(".pt"):
        raw = name[len("epoch") : -len(".pt")]
        return int(raw) if raw.isdigit() else None
    prefix = name.split("__", 1)[0]
    return int(prefix) if prefix.isdigit() else None


def write_checkpoint_manifest(
    *,
    run_dir: str,
    model_id: str,
    checkpoint_paths: Iterable[str],
) -> str:
    entries: list[TrainingCheckpointEntry] = []
    for checkpoint_path in checkpoint_paths:
        try:
            size, mtime = _stable_stat(checkpoint_path)
        except FileNotFoundError:
            continue
        entries.append(
            TrainingCheckpointEntry(
                path=str(checkpoint_path),
                epoch=infer_epoch_from_path(str(checkpoint_path)),
                size=size,
                mtime=mtime,
            )
        )
    manifest = TrainingCheckpointManifest(
        run_id=model_id,
        model_id=model_id,
        run_dir=run_dir,
        checkpoints=entries,
    )
    path = manifest_path_for_run(run_dir)
    with fsspec.open(path, "w", **_storage_options(path)) as out:
        json.dump(
            {
                **asdict(manifest),
                "checkpoints": [asdict(item) for item in manifest.checkpoints],
            },
            out,
            indent=2,
            sort_keys=True,
        )
    return path


def discover_checkpoint_paths(run_dir: str) -> list[str]:
    fs, stripped_run_dir = fsspec.core.url_to_fs(run_dir, **_storage_options(run_dir))
    if not fs.exists(stripped_run_dir):
        return []
    paths: list[str] = []
    for path in fs.find(stripped_run_dir):
        if not fs.isfile(path):
            continue
        name = PurePosixPath(path).name
        if (name.startswith("epoch") and name.endswith(".pt")) or name in {"last.pt", "best.pt"}:
            paths.append(str(Pathy.fluid(run_dir) / _relative_posix_path(path, stripped_run_dir)))
            continue
        if name.endswith(".keras"):
            paths.append(str(Pathy.fluid(run_dir) / _relative_posix_path(path, stripped_run_dir)))
    return sorted(paths)


def read_checkpoint_manifest(path: str) -> Optional[TrainingCheckpointManifest]:
    fs, stripped = fsspec.core.url_to_fs(path, **_storage_options(path))
    if not fs.exists(stripped):
        return None
    with fs.open(stripped, "r") as src:
        raw = json.load(src)
    return TrainingCheckpointManifest(
        run_id=raw["run_id"],
        model_id=raw["model_id"],
        run_dir=raw["run_dir"],
        checkpoints=[
            TrainingCheckpointEntry(
                path=item["path"],
                epoch=item.get("epoch"),
                size=int(item["size"]),
                mtime=item.get("mtime"),
                complete=bool(item.get("complete", True)),
            )
            for item in raw.get("checkpoints", [])
        ],
    )


def verify_manifest_checkpoint(entry: TrainingCheckpointEntry) -> bool:
    try:
        size, _mtime = _stat_url(entry.path)
    except FileNotFoundError:
        return False
    return entry.complete and size == entry.size


def _iter_relative_files(src: str) -> list[str]:
    src_fs, src_path = fsspec.core.url_to_fs(src, **_storage_options(src))
    if src_fs.isfile(src_path):
        return [PurePosixPath(src_path).name]
    files = [file_path for file_path in src_fs.find(src_path) if src_fs.isfile(file_path)]
    return [_relative_posix_path(file_path, src_path) for file_path in files]


def _publish_tmp(dst_url: str, tmp_url: str) -> None:
    dst_fs, dst_path = fsspec.core.url_to_fs(dst_url, **_storage_options(dst_url))
    _tmp_fs, tmp_path = fsspec.core.url_to_fs(tmp_url, **_storage_options(tmp_url))
    dst_fs.makedirs(str(PurePosixPath(dst_path).parent), exist_ok=True)
    if hasattr(dst_fs, "mv"):
        dst_fs.mv(tmp_path, dst_path)
        return
    with fsspec.open(tmp_url, "rb", **_storage_options(tmp_url)) as src_file:
        with fsspec.open(dst_url, "wb", **_storage_options(dst_url)) as dst_file:
            dst_file.write(src_file.read())
    dst_fs.rm(tmp_path)


def _copy_stable_file(src_url: str, dst_url: str) -> bool:
    from datapipe_ml.core.files import copy_url_to_url

    before = _stable_stat(src_url)
    tmp_url = f"{dst_url}.tmp.{os.getpid()}.{time.time_ns()}"
    try:
        copy_url_to_url(src_url, tmp_url, label="training sync file", concurrency=1)
        after = _stable_stat(src_url)
        if before != after:
            return False
        _publish_tmp(dst_url, tmp_url)
        return True
    finally:
        tmp_fs, tmp_path = fsspec.core.url_to_fs(tmp_url, **_storage_options(tmp_url))
        if tmp_fs.exists(tmp_path):
            tmp_fs.rm(tmp_path)


def copy_tree_snapshot(src: str, dst: str) -> None:
    for relative_path in _iter_relative_files(src):
        src_url = str(Pathy.fluid(src) / relative_path)
        dst_url = str(Pathy.fluid(dst) / relative_path)
        if not _copy_stable_file(src_url, dst_url):
            logger.info("Skipping unstable training artifact during sync: %s", src_url)


class PeriodicSyncScheduler:
    """Poll-friendly gate for non-fatal periodic sync (Sky/Vast host pull, etc.)."""

    def __init__(self, interval_s: Optional[int]) -> None:
        self.interval_s = interval_s
        self._next_sync_at: Optional[float] = None
        if interval_s is not None and interval_s > 0:
            self._next_sync_at = time.time() + interval_s

    def maybe_run(self, sync_fn: Callable[[], None], *, label: str) -> None:
        if self._next_sync_at is None or time.time() < self._next_sync_at:
            return
        self._next_sync_at = time.time() + self.interval_s  # type: ignore[operator]
        try:
            sync_fn()
        except Exception:
            logger.exception("Training output sync failed during %s; will retry later", label)


def copy_tree_best_effort(src: str, dst: str, *, retries: int, retry_sleep_s: int) -> None:

    attempts = max(1, retries)
    for attempt in range(1, attempts + 1):
        try:
            copy_tree_snapshot(src, dst)
            return
        except Exception:
            if attempt >= attempts:
                raise
            time.sleep(retry_sleep_s)


class PeriodicTrainingSync:
    def __init__(self, *, src: str, dst: str, config: TrainingSyncConfig, model_id: Optional[str] = None):
        self.src = src
        self.dst = dst
        self.config = config
        self.model_id = model_id
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def _publish_manifest_non_fatal(self) -> None:
        if self.model_id is None:
            return
        dst_run_dir = str(Pathy.fluid(self.dst) / self.model_id)
        checkpoint_paths = discover_checkpoint_paths(dst_run_dir)
        if not checkpoint_paths:
            return
        try:
            write_checkpoint_manifest(
                run_dir=dst_run_dir,
                model_id=self.model_id,
                checkpoint_paths=checkpoint_paths,
            )
        except Exception:
            logger.exception("Training manifest publish failed during sync")

    def _sync_once_non_fatal(self, label: str) -> None:
        try:
            if self.src != self.dst:
                copy_tree_best_effort(
                    self.src,
                    self.dst,
                    retries=self.config.retries,
                    retry_sleep_s=self.config.retry_sleep_s,
                )
            self._publish_manifest_non_fatal()
        except Exception:
            logger.exception("Training output sync failed during %s; will retry later", label)

    def _loop(self) -> None:
        interval_s = self.config.interval_s
        if interval_s is None:
            return
        while not self._stop.wait(interval_s):
            self._sync_once_non_fatal("periodic sync")

    def start(self) -> None:
        if not self.config.enabled or self.config.interval_s is None:
            return
        if self._thread is not None:
            return
        self._thread = threading.Thread(target=self._loop, name="datapipe-training-sync", daemon=True)
        self._thread.start()

    def stop(self, *, final_sync: bool = True) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5)
            self._thread = None
        if self.config.enabled and final_sync:
            self._sync_once_non_fatal("final sync")

    def __enter__(self) -> "PeriodicTrainingSync":
        self.start()
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        self.stop(final_sync=True)
