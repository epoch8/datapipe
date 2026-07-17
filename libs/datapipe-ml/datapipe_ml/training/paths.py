from __future__ import annotations

import os
import tempfile
from pathlib import Path, PurePosixPath

REMOTE_ROOT_ENV = "DATAPIPE_ML_REMOTE_ROOT"
DEFAULT_REMOTE_ROOT = "/workspace/datapipe_ml"


def remote_training_root() -> str:
    return os.environ.get(REMOTE_ROOT_ENV, DEFAULT_REMOTE_ROOT)


def remote_input_path(*parts: str) -> str:
    return str(Path(remote_training_root()) / "input" / Path(*parts))


def remote_output_models_path() -> str:
    return str(Path(remote_training_root()) / "output" / "models")


def remote_signals_path() -> str:
    return str(Path(remote_training_root()) / "signals")


def default_tmp_folder() -> str:
    return f"{tempfile.gettempdir()}/"


def default_train_project_dir() -> str:
    return str(Path(default_tmp_folder().rstrip("/")) / "runs" / "train")


def is_default_tmp_folder(tmp_folder: str) -> bool:
    normalized = str(tmp_folder).rstrip("/") + "/"
    return normalized == default_tmp_folder()


def relative_posix_path(path: str, base: str) -> str:
    return str(PurePosixPath(path).relative_to(PurePosixPath(base)))
