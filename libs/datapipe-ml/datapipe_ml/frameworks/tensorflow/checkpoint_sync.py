from __future__ import annotations

from pathlib import PurePosixPath
from typing import Optional

import fsspec
from pathy import Pathy

from datapipe_ml.training.paths import relative_posix_path

TF_LAST_CHECKPOINT_SUFFIX = "__last.keras"


def is_tf_last_checkpoint_path(path: str) -> bool:
    return PurePosixPath(path).name.endswith(TF_LAST_CHECKPOINT_SUFFIX)


def discover_checkpoint_paths_in_run_dir(run_dir: str) -> list[str]:
    fs, stripped_run_dir = fsspec.core.url_to_fs(run_dir)
    if not fs.exists(stripped_run_dir):
        return []
    paths: list[str] = []
    for path in fs.find(stripped_run_dir):
        if not fs.isfile(path):
            continue
        if PurePosixPath(path).name.endswith(".keras"):
            paths.append(str(Pathy.fluid(run_dir) / relative_posix_path(path, stripped_run_dir)))
    return sorted(paths)


def infer_epoch_from_checkpoint_path(path: str) -> Optional[int]:
    prefix = PurePosixPath(path).name.split("__", 1)[0]
    return int(prefix) if prefix.isdigit() else None
