from __future__ import annotations

import os
from typing import Any

import fsspec


def s3_filedir_fsspec_kwargs() -> dict[str, Any] | None:
    endpoint = os.environ.get("S3_ENDPOINT_URL")
    if not endpoint:
        return None
    return {"client_kwargs": {"endpoint_url": endpoint}}


def fsspec_storage_options(url: str) -> dict[str, Any]:
    protocol, _path = fsspec.core.split_protocol(str(url))
    if protocol != "s3":
        return {}
    return s3_filedir_fsspec_kwargs() or {}
