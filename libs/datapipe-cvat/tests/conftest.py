import os
import tempfile
from pathlib import Path

import pytest


@pytest.fixture
def tmp_dir():
    with tempfile.TemporaryDirectory() as d:
        yield Path(d)


@pytest.fixture(scope="session")
def cvat_url() -> str:
    return os.environ.get("CVAT_URL", "http://localhost:8080")


@pytest.fixture(scope="session")
def cvat_credentials() -> tuple[str, str]:
    return (
        os.environ.get("CVAT_USERNAME", "admin"),
        os.environ.get("CVAT_PASSWORD", "admin"),
    )
