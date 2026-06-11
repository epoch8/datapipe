from __future__ import annotations

import os
import subprocess
import time
from pathlib import Path
from typing import Protocol
from unittest.mock import patch


FAIL_AFTER_EPOCH_ENV = "DATAPIPE_ML_TEST_FAIL_AFTER_EPOCH"
FAIL_MODE_ENV = "DATAPIPE_ML_TEST_FAIL_MODE"
WORK_DIR_ENV = "DATAPIPE_ML_TEST_WORKDIR"
RUN_DIR_MARKER = ".datapipe_test_run_dir"


class _AttrPatcher(Protocol):
    def setattr(self, target: object, name: str, value: object) -> None: ...


class _SetattrPatcher:
    def setattr(self, target: object, name: str, value: object) -> None:
        setattr(target, name, value)


def configured_fail_after_epoch() -> int | None:
    raw = os.environ.get(FAIL_AFTER_EPOCH_ENV)
    if not raw:
        return None
    value = int(raw)
    return value if value > 0 else None


def configured_fail_mode() -> str:
    return os.environ.get(FAIL_MODE_ENV, "error").strip() or "error"


def record_run_dir_for_pipe_death(run_dir: str | Path) -> None:
    workdir = os.environ.get(WORK_DIR_ENV)
    if not workdir or configured_fail_mode() != "kill_pipe":
        return
    Path(workdir, RUN_DIR_MARKER).write_text(str(run_dir))


def run_dir_for_pipe_death_poll() -> str | None:
    workdir = os.environ.get(WORK_DIR_ENV)
    if not workdir:
        return None
    marker = Path(workdir) / RUN_DIR_MARKER
    if not marker.exists():
        return None
    text = marker.read_text().strip()
    return text or None


def maybe_fail_after_epoch(epoch: int) -> None:
    fail_after = configured_fail_after_epoch()
    if fail_after is None or epoch < fail_after:
        return
    mode = configured_fail_mode()
    if mode == "kill9":
        os._exit(137)
    if mode == "kill_pipe":
        return
    raise RuntimeError(f"Injected training failure after epoch {epoch}")


def checkpoint_for_epoch_exists(run_dir: str | Path, epoch: int, *, strict: bool = False) -> bool:
    weights_dir = Path(run_dir) / "weights"
    if (weights_dir / f"epoch{epoch}.pt").exists():
        return True
    if strict:
        return False
    return (weights_dir / "last.pt").exists()


def install_training_failure_hooks(patcher: _AttrPatcher) -> None:
    _install_yolov8_failure_hook(patcher)
    _install_yolov5_failure_hook(patcher)
    _install_tensorflow_failure_hook(patcher)


def install_training_failure_hooks_direct() -> None:
    if configured_fail_after_epoch() is None:
        return
    install_training_failure_hooks(_SetattrPatcher())


def run_training_with_failure_hooks(queue, target, *args):  # noqa: ANN001
    install_training_failure_hooks_direct()
    return target(queue, *args)


def _install_yolov8_failure_hook(patcher: _AttrPatcher) -> None:
    try:
        import ultralytics
    except ImportError:
        return

    original_yolo = ultralytics.YOLO

    def yolo_with_optional_failure(*args, **kwargs):
        model = original_yolo(*args, **kwargs)
        if configured_fail_after_epoch() is not None:

            def on_model_save(trainer) -> None:  # noqa: ANN001
                save_dir = getattr(trainer, "save_dir", None)
                if save_dir:
                    record_run_dir_for_pipe_death(save_dir)
                maybe_fail_after_epoch(int(trainer.epoch) + 1)

            model.add_callback("on_model_save", on_model_save)
        return model

    patcher.setattr(ultralytics, "YOLO", yolo_with_optional_failure)


def _install_yolov5_failure_hook(patcher: _AttrPatcher) -> None:
    try:
        import datapipe_ml.frameworks.yolo.yolov5.runner as runner
    except ImportError:
        return

    original_train_model = runner.train_model

    def train_model_with_optional_failure(*args, **kwargs):
        if configured_fail_after_epoch() is None:
            return original_train_model(*args, **kwargs)

        yolov5_training_config = kwargs.get("yolov5_training_config")
        if yolov5_training_config is None and args:
            yolov5_training_config = args[0]
        if yolov5_training_config is None:
            raise TypeError("yolov5 failure hook expected yolov5_training_config")

        def subprocess_run_with_failure_monitor(command, env=None):
            process = subprocess.Popen(command, env=env)
            fail_after = configured_fail_after_epoch()
            while process.poll() is None:
                if fail_after is not None:
                    run_dir = Path(str(yolov5_training_config.project)) / yolov5_training_config.name
                    strict_checkpoint = configured_fail_mode() == "kill_pipe"
                    if checkpoint_for_epoch_exists(run_dir, fail_after, strict=strict_checkpoint):
                        if configured_fail_mode() == "kill_pipe":
                            record_run_dir_for_pipe_death(run_dir)
                        if configured_fail_mode() == "kill9":
                            process.kill()
                            os._exit(137)
                        if configured_fail_mode() == "kill_pipe":
                            continue
                        process.terminate()
                        process.wait(timeout=30)
                        raise RuntimeError(f"Injected training failure after epoch {fail_after}")
                time.sleep(1)
            return subprocess.CompletedProcess(command, process.returncode or 0)

        with patch.object(runner.subprocess, "run", side_effect=subprocess_run_with_failure_monitor):
            return original_train_model(*args, **kwargs)

    patcher.setattr(runner, "train_model", train_model_with_optional_failure)


def _install_tensorflow_failure_hook(patcher: _AttrPatcher) -> None:
    try:
        import tensorflow as tf
    except ImportError:
        return

    import datapipe_ml.frameworks.tensorflow.classification_runner as runner

    original_train_on_tensorflow = runner.train_on_tensorflow

    class InjectedFailureCallback(tf.keras.callbacks.Callback):
        def on_epoch_end(self, epoch, logs=None):  # noqa: ANN001
            model_dir = getattr(self, "model_dir", None)
            if model_dir:
                record_run_dir_for_pipe_death(model_dir)
            maybe_fail_after_epoch(int(epoch) + 1)

    def train_on_tensorflow_with_optional_failure(*args, **kwargs):
        if configured_fail_after_epoch() is None:
            return original_train_on_tensorflow(*args, **kwargs)
        if "callbacks" in kwargs:
            callbacks = list(kwargs["callbacks"])
            callbacks.append(InjectedFailureCallback())
            kwargs = {**kwargs, "callbacks": callbacks}
            return original_train_on_tensorflow(*args, **kwargs)
        args = list(args)
        args[5] = [*args[5], InjectedFailureCallback()]
        return original_train_on_tensorflow(*tuple(args), **kwargs)

    patcher.setattr(runner, "train_on_tensorflow", train_on_tensorflow_with_optional_failure)
