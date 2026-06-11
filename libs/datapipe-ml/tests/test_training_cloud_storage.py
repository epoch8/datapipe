from __future__ import annotations

import os
from pathlib import PurePosixPath

import fsspec
import pytest

from datapipe_ml.training.resume import select_resume_checkpoint
from datapipe_ml.training.specs import TrainingResumeConfig
from datapipe_ml.training.sync import write_checkpoint_manifest


def _s3_base_url() -> str:
    bucket = os.environ.get("S3_BUCKET")
    if not bucket:
        pytest.skip("S3_BUCKET is not configured")
    prefix = os.environ.get("S3_PREFIX", "datapipe-ml-tests")
    return f"s3://{bucket}/{prefix.strip('/')}"


def _s3_storage_options() -> dict:
    endpoint_url = os.environ.get("S3_ENDPOINT_URL")
    return {"client_kwargs": {"endpoint_url": endpoint_url}} if endpoint_url else {}


@pytest.mark.cloud_storage
@pytest.mark.service_e2e
def test_s3_minio_manifest_resume_checkpoint_roundtrip() -> None:
    base_url = _s3_base_url()
    storage_options = _s3_storage_options()
    fs, base_path = fsspec.core.url_to_fs(base_url, **storage_options)
    run_path = str(PurePosixPath(base_path) / "model-a")
    checkpoint_path = str(PurePosixPath(run_path) / "weights" / "epoch1.pt")
    fs.makedirs(str(PurePosixPath(checkpoint_path).parent), exist_ok=True)
    with fs.open(checkpoint_path, "wb") as out:
        out.write(b"checkpoint")

    protocol = fs.protocol[0] if isinstance(fs.protocol, tuple) else fs.protocol
    run_url = f"{protocol}://{run_path}"
    checkpoint_url = f"{protocol}://{checkpoint_path}"
    manifest_path = write_checkpoint_manifest(
        run_dir=run_url,
        model_id="model-a",
        checkpoint_paths=[checkpoint_url],
    )

    selected = select_resume_checkpoint(
        manifest_path=manifest_path,
        config=TrainingResumeConfig(continue_train_failed_models=True, min_completed_epochs=1),
    )

    assert selected is not None
    assert selected.path == checkpoint_url
