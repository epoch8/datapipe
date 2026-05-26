from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from datapipe.store.database import DBConn


def sqlite_dbconnstr(path: Path) -> str:
    return f"sqlite:///{path}"


@dataclass(frozen=True)
class ServiceSettings:
    dbconn: DBConn
    datapipe_dir: Path
    aws_key: str
    aws_secret: str
    aws_region: str
    s3_bucket: str
    s3_prefix: str
    label_studio_url: str
    label_studio_api_key: str
    s3_endpoint_url: str | None = None
    local_images_dir: Path | None = None

    @classmethod
    def from_env(cls) -> "ServiceSettings":
        datapipe_dir = Path(os.environ.get("DATAPIPE_E2E_DIR", "datapipe")).resolve()
        db_url = os.environ["DB_URL"]
        return cls(
            dbconn=DBConn(db_url, os.environ.get("DB_SCHEMA", "public")),
            datapipe_dir=datapipe_dir,
            aws_key=os.environ["AWS_ACCESS_KEY_ID"],
            aws_secret=os.environ["AWS_SECRET_ACCESS_KEY"],
            aws_region=os.environ.get("AWS_REGION", "us-east-1"),
            s3_bucket=os.environ["S3_BUCKET"],
            s3_prefix=os.environ.get("S3_PREFIX", "images"),
            label_studio_url=os.environ["LABEL_STUDIO_URL"],
            label_studio_api_key=os.environ["LABEL_STUDIO_API_KEY"],
            s3_endpoint_url=os.environ.get("S3_ENDPOINT_URL"),
            local_images_dir=Path(os.environ.get("LOCAL_IMAGES_DIR", datapipe_dir / "local_images")).resolve(),
        )


def make_sqlite_settings(
    tmp_path: Path,
    *,
    pipeline_name: str,
    label_studio_url: str = "http://localhost:8080",
    label_studio_api_key: str = "test-token",
) -> ServiceSettings:
    workdir = tmp_path / pipeline_name
    workdir.mkdir(parents=True, exist_ok=True)
    return ServiceSettings(
        dbconn=DBConn(sqlite_dbconnstr(workdir / "e2e.sqlite")),
        datapipe_dir=workdir,
        aws_key="minioadmin",
        aws_secret="minioadmin",
        aws_region="us-east-1",
        s3_bucket="datapipe-e2e",
        s3_prefix=f"{pipeline_name}/images",
        label_studio_url=label_studio_url,
        label_studio_api_key=label_studio_api_key,
        s3_endpoint_url="http://localhost:9000",
        local_images_dir=workdir / "local_images",
    )

