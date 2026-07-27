import logging
from dataclasses import dataclass
from typing import TypedDict

logger = logging.getLogger("dataipipe_label_studio_lite")


class ProjectDict(TypedDict):
    id: int
    title: str


class StorageDict(TypedDict, total=False):
    bucket: str


class ImportApiRequest(TypedDict):
    data: dict[str, object]


class ImportTasksResponseDict(TypedDict, total=False):
    task_ids: list[int]


@dataclass
class GCSBucket:
    bucket: str
    google_application_credentials: str | None = None

    @property
    def type(self):
        return "gcs"


@dataclass
class S3Bucket:
    bucket: str
    key: str
    secret: str
    region_name: str | None = None
    endpoint_url: str | None = None

    @property
    def type(self):
        return "s3"


@dataclass
class Buckets:
    buckets: list[GCSBucket | S3Bucket]
