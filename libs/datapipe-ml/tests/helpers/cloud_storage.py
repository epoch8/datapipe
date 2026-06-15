from __future__ import annotations

import os
import uuid
from pathlib import Path

import fsspec
import pytest
from pathy import FluidPath, Pathy

CloudPath = str | Path | Pathy


def _pathy(path: CloudPath) -> FluidPath:
    return Pathy.fluid(path)


def is_cloud_url(path: CloudPath) -> bool:
    protocol, _ = fsspec.core.split_protocol(str(path))
    return protocol not in (None, "file")


def _storage_options(url: CloudPath) -> dict:
    from datapipe_ml.utils.fsspec_storage import fsspec_storage_options

    return fsspec_storage_options(str(url))


def s3_storage_options() -> dict:
    return _storage_options("s3://")


def _require_s3_bucket() -> str:
    bucket = os.environ.get("S3_BUCKET")
    if not bucket:
        pytest.skip("S3_BUCKET is not configured")
    return bucket


def s3_base_url() -> Pathy:
    bucket = _require_s3_bucket()
    prefix = Path(os.environ.get("S3_PREFIX", "datapipe-ml-tests"))
    return Pathy(f"s3://{bucket}").joinpath(*prefix.parts)


def cloud_working_dir(*parts: str) -> Pathy:
    run_id = os.environ.get("GITHUB_RUN_ID", uuid.uuid4().hex[:12])
    path = s3_base_url() / "working-dirs" / run_id
    for part in parts:
        path = path.joinpath(*Path(part).parts)
    return path


def join_cloud_path(base: CloudPath, *parts: str) -> Pathy:
    path = _pathy(base)
    for part in parts:
        path = path.joinpath(*Path(part).parts)
    return path


def upload_local_file(local_path: CloudPath, dest: CloudPath) -> Pathy:
    dest_pathy = _pathy(dest)
    payload = Path(local_path).read_bytes()
    with fsspec.open(str(dest_pathy), "wb", **_storage_options(dest_pathy)) as out:
        out.write(payload)
    return dest_pathy


def assert_url_exists(url: CloudPath, *, is_file: bool = True) -> None:
    url_str = str(_pathy(url))
    fs, stripped = fsspec.core.url_to_fs(url_str, **_storage_options(url_str))
    if is_file:
        assert fs.isfile(stripped), url_str
        assert int(fs.info(stripped).get("size") or 0) > 0
    else:
        assert fs.exists(stripped), url_str


def assert_model_path_under_working_dir(model_path: CloudPath, working_dir: CloudPath) -> None:
    model = _pathy(model_path)
    workdir = _pathy(working_dir)
    try:
        model.relative_to(workdir)
    except ValueError as exc:
        raise AssertionError((str(model), str(workdir))) from exc
    assert_url_exists(model)
