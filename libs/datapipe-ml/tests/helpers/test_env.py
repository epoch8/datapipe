from __future__ import annotations

import os
from pathlib import Path

ENV_TEST_FILENAME = ".env.test"
ENV_TEST_LOCAL_FILENAME = ".env.test.local"


def _load_env_file(env_path: Path) -> None:
    if not env_path.exists():
        return
    for raw_line in env_path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip("\"'"))


def load_test_env() -> None:
    tests_dir = Path(__file__).resolve().parent.parent
    _load_env_file(tests_dir / ENV_TEST_FILENAME)
    _load_env_file(tests_dir / ENV_TEST_LOCAL_FILENAME)
