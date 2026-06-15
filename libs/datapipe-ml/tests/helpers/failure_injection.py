from __future__ import annotations

import multiprocessing as mp
import os
import queue
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from datapipe_ml.training.sync import manifest_path_for_run, read_checkpoint_manifest

from tests.helpers.failure_injection_bootstrap import (
    FAIL_AFTER_EPOCH_ENV,
    FAIL_MODE_ENV,
    checkpoint_for_epoch_exists,
    configured_fail_after_epoch,
    configured_fail_mode,
    install_training_failure_hooks as _install_training_failure_hooks,
    install_training_failure_hooks_direct,
    maybe_fail_after_epoch,
    run_dir_for_pipe_death_poll,
    run_training_with_failure_hooks,
)

if TYPE_CHECKING:
    from _pytest.monkeypatch import MonkeyPatch


class _MonkeyPatchAdapter:
    def __init__(self, monkeypatch: MonkeyPatch) -> None:
        self._monkeypatch = monkeypatch

    def setattr(self, target: object, name: str, value: object) -> None:
        self._monkeypatch.setattr(target, name, value)


def enable_failure_after_epoch(monkeypatch: MonkeyPatch, *, epoch: int, mode: str = "error") -> None:
    monkeypatch.setenv(FAIL_AFTER_EPOCH_ENV, str(epoch))
    monkeypatch.setenv(FAIL_MODE_ENV, mode)
    install_training_failure_hooks(monkeypatch)


def _pipe_death_checkpoint_ready(run_dir: str, fail_after: int) -> bool:
    if not checkpoint_for_epoch_exists(run_dir, fail_after, strict=True):
        return False
    manifest = read_checkpoint_manifest(manifest_path_for_run(run_dir))
    if manifest is None:
        return False
    epochs = [item.epoch for item in manifest.checkpoints if item.epoch is not None]
    return bool(epochs) and max(int(epoch) for epoch in epochs) >= fail_after


def _spawn_with_pipe_death(target, *args):  # noqa: ANN001
    ctx = mp.get_context("spawn")
    q: mp.Queue = ctx.Queue()
    p = ctx.Process(target=run_training_with_failure_hooks, args=(q, target, *args))
    p.start()
    fail_after = configured_fail_after_epoch()
    try:
        while True:
            try:
                res = q.get(timeout=0.2)
                break
            except queue.Empty:
                if not p.is_alive():
                    p.join()
                    raise RuntimeError(
                        f"Training subprocess exited before returning a result. exitcode={p.exitcode}"
                    )
                run_dir = run_dir_for_pipe_death_poll()
                if fail_after is not None and run_dir and _pipe_death_checkpoint_ready(run_dir, fail_after):
                    if p.is_alive():
                        p.kill()
                        p.join(timeout=30)
                    os._exit(137)
        p.join()
        return res
    finally:
        if p.is_alive():
            p.join()


def install_training_failure_hooks(monkeypatch: MonkeyPatch) -> None:
    import datapipe_ml.core.multiprocessing as mp_module

    if configured_fail_mode() == "kill_pipe":
        monkeypatch.setattr(mp_module, "_spawn", _spawn_with_pipe_death)
    else:
        original_spawn = mp_module._spawn

        def spawn_with_failure_hooks(target, *args, **kwargs):  # noqa: ANN001
            return original_spawn(run_training_with_failure_hooks, target, *args, **kwargs)

        monkeypatch.setattr(mp_module, "_spawn", spawn_with_failure_hooks)
    _install_training_failure_hooks(_MonkeyPatchAdapter(monkeypatch))


def install_pipe_death_hooks_direct() -> None:
    import datapipe_ml.core.multiprocessing as mp_module

    mp_module._spawn = _spawn_with_pipe_death
    install_training_failure_hooks_direct()


def remote_failure_hooks_bootstrap_source() -> str:
    return Path(__file__).with_name("failure_injection_bootstrap.py").read_text()


def prepend_remote_failure_hooks_to_worker_entrypoint(worker_source: str) -> str:
    bootstrap = remote_failure_hooks_bootstrap_source()
    return (
        f"_DATAPPIPE_FAILURE_HOOKS = {bootstrap!r}\n"
        "_DATAPPIPE_FAILURE_HOOK_SCOPE = {}\n"
        "exec(_DATAPPIPE_FAILURE_HOOKS, _DATAPPIPE_FAILURE_HOOK_SCOPE)\n"
        "_DATAPPIPE_FAILURE_HOOK_SCOPE['install_training_failure_hooks_direct']()\n"
        f"{worker_source}"
    )


def patch_sky_vast_worker_entrypoint_for_failure_hooks(monkeypatch: MonkeyPatch) -> None:
    original_read_text = Path.read_text

    def read_text_with_failure_hooks(self: Path, *args, **kwargs):  # noqa: ANN001
        text = original_read_text(self, *args, **kwargs)
        if self.name == "worker_entrypoint.py":
            return prepend_remote_failure_hooks_to_worker_entrypoint(text)
        return text

    monkeypatch.setattr(Path, "read_text", read_text_with_failure_hooks)


@pytest.fixture
def training_failure_hooks(monkeypatch: MonkeyPatch) -> None:
    install_training_failure_hooks(monkeypatch)
