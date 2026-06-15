import logging
import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import List

import fsspec
from fsspec import AbstractFileSystem
from pathy import Pathy

from datapipe_ml.utils.fsspec_storage import fsspec_storage_options

logger = logging.getLogger(__name__)


def same_filesystem_path(src_fs: AbstractFileSystem, src: str, dst_fs: AbstractFileSystem, dst: str) -> bool:
    src_path = src_fs._strip_protocol(src)
    dst_path = dst_fs._strip_protocol(dst)
    return src_fs.protocol == dst_fs.protocol and os.path.normpath(src_path) == os.path.normpath(dst_path)


def _copy_between_fs(
    src_fs: AbstractFileSystem,
    src: str,
    dst_fs: AbstractFileSystem,
    dst: str,
    *,
    label: str,
    concurrency: int = 8,
) -> None:
    if same_filesystem_path(src_fs, src, dst_fs, dst):
        logger.info("Skipping copy %s: %s already at %s", label, src, dst)
        return
    logger.info("Copying %s: %s -> %s", label, src, dst)
    src_path = src_fs._strip_protocol(src)
    dst_path = dst_fs._strip_protocol(dst)

    def _copy_file(src_file: str, dst_file: str) -> None:
        dst_fs.makedirs(str(Path(dst_file).parent), exist_ok=True)
        with src_fs.open(src_file, "rb") as in_file:
            with dst_fs.open(dst_file, "wb") as out_file:
                out_file.write(in_file.read())

    if src_fs.isfile(src_path):
        _copy_file(src_path, dst_path)
        return

    dst_fs.makedirs(dst_path, exist_ok=True)
    file_paths = [file_path for file_path in src_fs.find(src_path) if src_fs.isfile(file_path)]

    def _copy_tree_file(file_path: str) -> None:
        relative = os.path.relpath(file_path, src_path)
        _copy_file(file_path, str(Path(dst_path) / relative))

    if concurrency <= 1 or len(file_paths) <= 1:
        for file_path in file_paths:
            _copy_tree_file(file_path)
        return

    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        list(executor.map(_copy_tree_file, file_paths))


def copy_tree_between_fs(
    src_fs: AbstractFileSystem,
    src: str,
    dst_fs: AbstractFileSystem,
    dst: str,
    *,
    concurrency: int = 8,
) -> None:
    _copy_between_fs(src_fs, src, dst_fs, dst, label="tree", concurrency=concurrency)


def copy_url_to_fs(
    src: str,
    dst_fs: AbstractFileSystem,
    dst: str,
    *,
    label: str = "file",
    concurrency: int = 8,
) -> None:
    src_fs, src_path = fsspec.core.url_to_fs(src, **fsspec_storage_options(src))
    _copy_between_fs(src_fs, src_path, dst_fs, dst, label=label, concurrency=concurrency)


def copy_fs_to_url(
    src_fs: AbstractFileSystem,
    src: str,
    dst: str,
    *,
    label: str = "file",
    concurrency: int = 8,
) -> None:
    dst_fs, dst_path = fsspec.core.url_to_fs(dst, **fsspec_storage_options(dst))
    _copy_between_fs(src_fs, src, dst_fs, dst_path, label=label, concurrency=concurrency)


def copy_url_to_url(src: str, dst: str, *, label: str = "file", concurrency: int = 8) -> None:
    src_fs, src_path = fsspec.core.url_to_fs(src, **fsspec_storage_options(src))
    dst_fs, dst_path = fsspec.core.url_to_fs(dst, **fsspec_storage_options(dst))
    _copy_between_fs(src_fs, src_path, dst_fs, dst_path, label=label, concurrency=concurrency)


def _copy_filepath_to_folder(src_filepath: str, dst_folder_filepath: str) -> str:
    src_filepath_pathy = Pathy.fluid(src_filepath)
    frozen_dataset_id = src_filepath_pathy.parent.parent.name
    subset_id = src_filepath_pathy.parent.name
    brickit_part_num_tar = src_filepath_pathy.name
    dst_filepath = Pathy.fluid(dst_folder_filepath) / frozen_dataset_id / subset_id / brickit_part_num_tar
    copy_url_to_url(str(src_filepath), str(dst_filepath), label="file", concurrency=1)
    return str(dst_filepath)


def copy_filepath_to_folder(
    src_filepath: str,
    dst_folder_filepath: str,
    buf_size: int = 16 * 10**6,
) -> str:
    return _copy_filepath_to_folder(src_filepath, dst_folder_filepath)


def parallel_copy_filepaths_to_folder(
    src_filepaths: List[str],
    dst_folder_filepath: str,
    buf_size: int = 16 * 10**6,
    max_workers: int = 16,
) -> List[str]:
    logger.info("Copying %s files...", len(src_filepaths))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        return list(
            executor.map(
                lambda filepath: _copy_filepath_to_folder(str(filepath), str(dst_folder_filepath)),
                src_filepaths,
            )
        )
