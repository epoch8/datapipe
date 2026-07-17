from __future__ import annotations

import importlib.util
import tarfile
import tempfile
from pathlib import Path

from fsspec import AbstractFileSystem


def _package_root(import_name: str, *, project_file: str = "pyproject.toml") -> Path:
    spec = importlib.util.find_spec(import_name)
    if spec is None or spec.submodule_search_locations is None:
        raise RuntimeError(f"Could not find package {import_name!r} to copy Sky/Vast source")
    path = Path(next(iter(spec.submodule_search_locations))).resolve()
    for parent in (path.parent, *path.parents):
        if (parent / project_file).exists():
            return parent
    raise RuntimeError(f"Could not find {project_file} for package {import_name!r}")


def copy_project_source_to_remote(sshfs: AbstractFileSystem, dst: str) -> None:
    datapipe_core_root = _package_root("datapipe")
    datapipe_ml_root = _package_root("datapipe_ml")
    datapipe_core_readme = (datapipe_core_root / "../../README.md").resolve()
    source_paths = (
        (datapipe_core_readme, "README.md"),
        (datapipe_core_root / "pyproject.toml", "datapipe-core/pyproject.toml"),
        (datapipe_core_root / "datapipe", "datapipe-core/datapipe"),
        (datapipe_ml_root / "pyproject.toml", "datapipe-ml/pyproject.toml"),
        (datapipe_ml_root / "README.md", "datapipe-ml/README.md"),
        (datapipe_ml_root / "datapipe_ml", "datapipe-ml/datapipe_ml"),
    )
    with tempfile.NamedTemporaryFile(suffix=".tar.gz") as archive:
        with tarfile.open(archive.name, "w:gz") as tar:
            for local_path, archive_name in source_paths:
                if local_path.exists():
                    tar.add(local_path, arcname=archive_name)
        with open(archive.name, "rb") as in_file:
            with sshfs.open(dst, "wb") as out_file:
                out_file.write(in_file.read())
