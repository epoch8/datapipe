import inspect
import logging
import os
import shutil
import tempfile
import threading
import time
from pathlib import Path, PurePosixPath
from typing import Any, Dict, List, Optional, Union

import fsspec
from pathy import Pathy
from tensorflow.keras.callbacks import Callback, ModelCheckpoint

from datapipe_ml.core.atomic_io import publish_file_atomically
from datapipe_ml.frameworks.tensorflow.checkpoint_sync import TF_LAST_CHECKPOINT_SUFFIX

logger = logging.getLogger(__name__)


def _copy_file_fsspec(src_path: str, dst_url: str, chunk_bytes: int = 64 * 1024 * 1024) -> None:
    """Atomically publish a completed local checkpoint to its training destination."""
    del chunk_bytes
    publish_file_atomically(src_path, dst_url, label="TensorFlow checkpoint")


def _stage_checkpoint_for_upload(local_path: Path, staging_root: Path) -> Path:
    """Pin checkpoint bytes for background upload (hardlink when possible)."""
    staging_root.mkdir(parents=True, exist_ok=True)
    staged_path = staging_root / local_path.name
    if staged_path.exists():
        staged_path = staging_root / f"{time.time_ns()}_{local_path.name}"
    try:
        os.link(local_path, staged_path)
    except OSError:
        shutil.copy2(local_path, staged_path)
    return staged_path


def _safe_format(pattern: str, epoch: int, logs: Optional[Dict[str, Any]]) -> str:
    logs = logs or {}
    safe_logs: Dict[str, Any] = {}
    for k, v in logs.items():
        try:
            safe_logs[k] = float(v)
        except Exception:
            safe_logs[k] = v
    try:
        return pattern.format(epoch=epoch + 1, **safe_logs)
    except Exception:
        try:
            return pattern.format(epoch=epoch + 1)
        except Exception:
            return pattern


class FsspecModelCheckpoint(Callback):
    def __init__(
        self,
        filepath: Optional[str] = None,
        remote_filepath_pattern: Optional[str] = None,
        monitor: str = "val_loss",
        verbose: int = 0,
        save_best_only: bool = False,
        save_weights_only: bool = False,
        mode: str = "auto",
        save_freq: Union[str, int] = "epoch",
        initial_value_threshold: Optional[float] = None,
        options: Optional[Any] = None,  # ignored when the installed Keras version does not support it
        mirror_async: bool = True,
        rolling_last: bool = False,
        **kwargs,
    ):
        super().__init__(**kwargs)

        # Support both parameter names for backward compatibility.
        self.remote_filepath_pattern = remote_filepath_pattern or filepath
        if not self.remote_filepath_pattern:
            raise ValueError("You must pass `filepath` (alias: `remote_filepath_pattern`).")

        self.verbose = int(verbose)
        self.rolling_last = rolling_last

        # Temp directory for the full training run.
        self._tmpdir = tempfile.TemporaryDirectory()
        base_name = Pathy.fluid(self.remote_filepath_pattern).name
        self._local_pattern = str(Path(self._tmpdir.name) / base_name)

        # Forward only arguments supported by the installed ModelCheckpoint.
        params = inspect.signature(ModelCheckpoint.__init__).parameters
        inner_kwargs = dict(
            filepath=self._local_pattern,
            monitor=monitor,
            verbose=verbose,
            save_best_only=save_best_only,
            save_weights_only=save_weights_only,
            mode=mode,
            save_freq=save_freq,
        )
        if "initial_value_threshold" in params and initial_value_threshold is not None:
            inner_kwargs["initial_value_threshold"] = initial_value_threshold
        if "options" in params and options is not None:
            inner_kwargs["options"] = options
        # Forward only kwargs supported by the installed Keras version.
        for k, v in list(kwargs.items()):
            if k in params:
                inner_kwargs[k] = v

        self._inner_ckpt = ModelCheckpoint(**inner_kwargs)
        self._chunk_bytes = 64 * 1024 * 1024
        self._mirror_async = mirror_async
        self._staging_root = Path(self._tmpdir.name) / "mirror_staging"
        self._copy_threads: List[threading.Thread] = []
        self._copy_threads_lock = threading.Lock()

    # Lifecycle delegation
    def set_model(self, model):
        self._inner_ckpt.set_model(model)
        return super().set_model(model)

    def on_train_begin(self, logs=None):
        self._inner_ckpt.on_train_begin(logs)

    def on_train_end(self, logs=None):
        self._inner_ckpt.on_train_end(logs)
        self._wait_for_mirror_threads()
        self._tmpdir.cleanup()

    def _wait_for_mirror_threads(self) -> None:
        with self._copy_threads_lock:
            pending_threads = list(self._copy_threads)
        if pending_threads and self.verbose:
            print("FsspecModelCheckpoint: waiting for mirror copy threads...")
        for thread in pending_threads:
            thread.join()
        with self._copy_threads_lock:
            self._copy_threads.clear()

    def on_epoch_end(self, epoch, logs=None):
        self._inner_ckpt.on_epoch_end(epoch, logs)
        self._mirror_if_exists(epoch, logs)

    def on_train_batch_end(self, batch, logs=None):
        self._inner_ckpt.on_train_batch_end(batch, logs)
        epoch = self.params.get("epoch", 0) if isinstance(self.params, dict) else 0
        self._mirror_if_exists(epoch, logs)

    def _resolve_local_checkpoint_path(self, epoch: int, logs: Optional[Dict[str, Any]]) -> Optional[Path]:
        inner_path = getattr(self._inner_ckpt, "filepath", None)
        if inner_path:
            candidate = Path(inner_path)
            if candidate.exists():
                return candidate

        formatted_name = _safe_format(Path(self._local_pattern).name, epoch, logs)
        candidate = Path(self._tmpdir.name) / formatted_name
        if candidate.exists():
            return candidate

        logger.debug(
            "FsspecModelCheckpoint: no local checkpoint resolved for epoch=%s",
            epoch,
        )
        return None

    def _delete_stale_last_checkpoints(self, keep_remote_url: str) -> None:
        if not self.rolling_last:
            return
        remote_fs, remote_path = fsspec.core.url_to_fs(keep_remote_url)
        keep_name = PurePosixPath(remote_path).name
        parent = str(PurePosixPath(remote_path).parent)
        for candidate in remote_fs.glob(f"{parent}/*{TF_LAST_CHECKPOINT_SUFFIX}"):
            if PurePosixPath(candidate).name == keep_name:
                continue
            remote_fs.rm(candidate)

    # Mirror checkpoints to remote storage.
    def _mirror_if_exists(self, epoch: int, logs: Optional[Dict[str, Any]]):
        local_path = self._resolve_local_checkpoint_path(epoch, logs)
        if local_path is None:
            return

        assert isinstance(
            self.remote_filepath_pattern, str
        ), f"remote_filepath_pattern must be a string, got {type(self.remote_filepath_pattern)}"
        remote_url = _safe_format(self.remote_filepath_pattern, epoch, logs)

        if self.verbose:
            print(f"FsspecModelCheckpoint: mirroring '{local_path}' -> '{remote_url}'")

        if self._mirror_async:
            staged_path = _stage_checkpoint_for_upload(local_path, self._staging_root)

            def _upload() -> None:
                try:
                    _copy_file_fsspec(str(staged_path), remote_url, self._chunk_bytes)
                    if self.rolling_last:
                        self._delete_stale_last_checkpoints(remote_url)
                finally:
                    staged_path.unlink(missing_ok=True)

            thread = threading.Thread(
                target=_upload,
                name="fsspec-checkpoint-mirror",
                daemon=False,
            )
            with self._copy_threads_lock:
                self._copy_threads.append(thread)
            thread.start()
        else:
            _copy_file_fsspec(str(local_path), remote_url, self._chunk_bytes)
            if self.rolling_last:
                self._delete_stale_last_checkpoints(remote_url)
