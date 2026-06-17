from __future__ import annotations

import io
import zipfile
from pathlib import Path


def write_valid_zip_checkpoint(path: Path, *, label: str = "checkpoint") -> None:
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("checkpoint.txt", label)


def write_corrupt_zip_checkpoint(path: Path) -> None:
    path.write_bytes(b"PK\x03\x04" + b"truncated-zip-payload")
