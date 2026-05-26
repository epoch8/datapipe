import inspect
import multiprocessing
import os
import tempfile
from typing import Any, Dict, List, Optional, Union

from tensorflow.keras.callbacks import Callback, ModelCheckpoint

from datapipe_ml.core.files import copy_url_to_url


def _copy_file_fsspec(src_path: str, dst_url: str, chunk_bytes: int = 64 * 1024 * 1024) -> None:
    copy_url_to_url(src_path, dst_url, label="TensorFlow checkpoint", concurrency=1)


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
        options: Optional[Any] = None,  # будет проигнорирован, если не поддерживается
        **kwargs,
    ):
        super().__init__(**kwargs)

        # Поддерживаем оба имени параметра для совместимости с твоим кодом
        self.remote_filepath_pattern = remote_filepath_pattern or filepath
        if not self.remote_filepath_pattern:
            raise ValueError("You must pass `filepath` (alias: `remote_filepath_pattern`).")

        self.verbose = int(verbose)

        # temp-директория на весь ран
        self._tmpdir = tempfile.TemporaryDirectory()
        base_name = os.path.basename(self.remote_filepath_pattern)
        self._local_pattern = os.path.join(self._tmpdir.name, base_name)

        # Аккуратно прокидываем только поддерживаемые аргументы
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
        # Прокидываем из **kwargs только то, что поддерживается текущей версией
        for k, v in list(kwargs.items()):
            if k in params:
                inner_kwargs[k] = v

        self._inner_ckpt = ModelCheckpoint(**inner_kwargs)

        self._copy_processes: List[multiprocessing.Process] = []
        self._chunk_bytes = 64 * 1024 * 1024
        self._async_ok = multiprocessing.get_start_method(allow_none=True) == "spawn"

    # Делегирование жизненного цикла
    def set_model(self, model):
        self._inner_ckpt.set_model(model)
        return super().set_model(model)

    def on_train_begin(self, logs=None):
        self._inner_ckpt.on_train_begin(logs)

    def on_train_end(self, logs=None):
        self._inner_ckpt.on_train_end(logs)
        if self._copy_processes:
            print("FsspecModelCheckpoint: waiting for mirror copy processes...")
        for p in self._copy_processes:
            p.join()
        self._tmpdir.cleanup()

    def on_epoch_end(self, epoch, logs=None):
        self._inner_ckpt.on_epoch_end(epoch, logs)
        self._mirror_if_exists(epoch, logs)

    def on_train_batch_end(self, batch, logs=None):
        self._inner_ckpt.on_train_batch_end(batch, logs)
        epoch = self.params.get("epoch", 0) if isinstance(self.params, dict) else 0
        self._mirror_if_exists(epoch, logs)

    # Внутреннее зеркалирование
    def _mirror_if_exists(self, epoch: int, logs: Optional[Dict[str, Any]]):
        local_filename = _safe_format(os.path.basename(self._local_pattern), epoch, logs)
        local_path = os.path.join(self._tmpdir.name, local_filename)

        if not os.path.exists(local_path):
            try:
                candidates = [os.path.join(self._tmpdir.name, f) for f in os.listdir(self._tmpdir.name)]
                candidates = [p for p in candidates if os.path.isfile(p)]
                if not candidates:
                    return
                local_path = max(candidates, key=os.path.getmtime)
            except Exception:
                return

        assert isinstance(
            self.remote_filepath_pattern, str
        ), f"remote_filepath_pattern must be a string, got {type(self.remote_filepath_pattern)}"
        remote_url = _safe_format(self.remote_filepath_pattern, epoch, logs)

        if self.verbose:
            print(f"FsspecModelCheckpoint: mirroring '{local_path}' -> '{remote_url}'")

        if self._async_ok:
            p = multiprocessing.Process(target=_copy_file_fsspec, args=(local_path, remote_url, self._chunk_bytes))
            p.start()
            self._copy_processes.append(p)
        else:
            _copy_file_fsspec(local_path, remote_url, self._chunk_bytes)
