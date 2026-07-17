import logging
from dataclasses import dataclass
from typing import Dict, List, Optional, TypedDict, Union

logger = logging.getLogger("dataipipe_label_studio_lite")


class ProjectDict(TypedDict):
    id: int
    title: str


class StorageDict(TypedDict, total=False):
    bucket: str


class ImportApiRequest(TypedDict):
    data: Dict[str, object]


class ImportTasksResponseDict(TypedDict, total=False):
    task_ids: List[int]


@dataclass
class GCSBucket:
    bucket: str
    google_application_credentials: Optional[str] = None

    @property
    def type(self):
        return "gcs"


@dataclass
class S3Bucket:
    bucket: str
    key: str
    secret: str
    region_name: Optional[str] = None
    endpoint_url: Optional[str] = None

    @property
    def type(self):
        return "s3"


@dataclass
class Buckets:
    buckets: List[Union[GCSBucket, S3Bucket]]
