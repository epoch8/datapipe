from __future__ import annotations

import os
from pathlib import Path

ENV_TEST_FILENAME = ".env.test"


def load_test_env() -> None:
    env_path = Path(__file__).resolve().parent.parent / ENV_TEST_FILENAME
    if not env_path.exists():
        return
    for raw_line in env_path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip("\"'"))
