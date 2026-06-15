from __future__ import annotations

import logging
import tempfile
import shutil
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, Iterable, Optional

from pathy import Pathy

from datapipe_ml.core.files import copy_url_to_url, parallel_copy_filepaths_to_folder

logger = logging.getLogger("datapipe.ml.training.staging")


@dataclass
class LocalStagingDir:
    path: Path
    _tmpdir: Optional[tempfile.TemporaryDirectory] = None
    remove_on_cleanup: bool = False

    def cleanup(self) -> None:
        if self._tmpdir is not None:
            self._tmpdir.cleanup()
        elif self.remove_on_cleanup:
            shutil.rmtree(self.path, ignore_errors=True)


def make_local_staging_dir(
    *,
    tmp_folder: str,
    name: str,
    use_managed_tmp: bool,
    remove_on_cleanup: bool = False,
) -> LocalStagingDir:
    if use_managed_tmp:
        tmpdir = tempfile.TemporaryDirectory()
        return LocalStagingDir(path=Path(tmpdir.name), _tmpdir=tmpdir)

    path = Path(tmp_folder) / name
    path.mkdir(exist_ok=True, parents=True)
    return LocalStagingDir(path=path, remove_on_cleanup=remove_on_cleanup)


def stage_files_to_local_folder(filepaths: Iterable[str], dst_folder: Path, *, label: str) -> list[str]:
    logger.info("Parallel: copying %s to local tmp folder dst_folder=%s", label, dst_folder)
    return parallel_copy_filepaths_to_folder(list(filepaths), str(dst_folder))


@contextmanager
def copied_file_to_local_temp(src: str, *, suffix: str, label: str) -> Iterator[Path]:
    with tempfile.NamedTemporaryFile(suffix=suffix) as tmpfile:
        copy_url_to_url(src, tmpfile.name, label=label, concurrency=1)
        yield Path(tmpfile.name)


def copy_file_to_local_staging(src: str, staging_dir: LocalStagingDir, *, label: str) -> Path:
    dst = staging_dir.path / Pathy.fluid(src).name
    copy_url_to_url(src, str(dst), label=label, concurrency=1)
    return dst
