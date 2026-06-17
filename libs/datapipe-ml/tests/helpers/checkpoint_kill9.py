from __future__ import annotations

import multiprocessing as mp
import os
import signal
import time
from pathlib import Path

from tests.helpers.checkpoint_fixtures import write_corrupt_zip_checkpoint, write_valid_zip_checkpoint


def _child_atomic_save_before_replace(checkpoint_path: str, ready_queue: mp.Queue) -> None:
    from datapipe_ml.core.atomic_io import atomic_write_local

    path = Path(checkpoint_path)
    write_valid_zip_checkpoint(path, label="previous")
    with atomic_write_local(path) as tmp_path:
        write_valid_zip_checkpoint(tmp_path, label="new")
        ready_queue.put("ready")
        time.sleep(120)


def _child_non_atomic_overwrite(checkpoint_path: str, ready_queue: mp.Queue) -> None:
    path = Path(checkpoint_path)
    write_valid_zip_checkpoint(path, label="previous")
    ready_queue.put("ready")
    with open(path, "wb") as handle:
        handle.write(b"PK\x03\x04")
        handle.flush()
        os.fsync(handle.fileno())
        time.sleep(120)


def kill9_during_atomic_checkpoint_save(checkpoint_path: Path) -> None:
    ctx = mp.get_context("spawn")
    ready_queue: mp.Queue = ctx.Queue()
    process = ctx.Process(target=_child_atomic_save_before_replace, args=(str(checkpoint_path), ready_queue))
    process.start()
    try:
        assert ready_queue.get(timeout=10) == "ready"
        os.kill(process.pid, signal.SIGKILL)
        process.join(timeout=10)
        assert not process.is_alive()
    finally:
        if process.is_alive():
            process.kill()
            process.join(timeout=5)


def kill9_during_non_atomic_checkpoint_save(checkpoint_path: Path) -> None:
    ctx = mp.get_context("spawn")
    ready_queue: mp.Queue = ctx.Queue()
    process = ctx.Process(target=_child_non_atomic_overwrite, args=(str(checkpoint_path), ready_queue))
    process.start()
    try:
        assert ready_queue.get(timeout=10) == "ready"
        os.kill(process.pid, signal.SIGKILL)
        process.join(timeout=10)
        assert not process.is_alive()
    finally:
        if process.is_alive():
            process.kill()
            process.join(timeout=5)
