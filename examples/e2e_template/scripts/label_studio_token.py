#!/usr/bin/env python3
from __future__ import annotations

import os
import sys

from datapipe_label_studio.sdk_utils import login_and_get_token, sign_up


def main() -> int:
    url = os.environ.get("LABEL_STUDIO_URL", "http://localhost:8080")
    email = os.environ.get("LABEL_STUDIO_EMAIL", "e2e@example.com")
    password = os.environ.get("LABEL_STUDIO_PASSWORD", "qwerty123")

    token = sign_up(url, email, password) or login_and_get_token(url, email, password)
    if not token:
        print("Failed to obtain Label Studio API token", file=sys.stderr)
        return 1

    print(token)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
