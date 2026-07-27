#!/usr/bin/env python3
"""Download the built-in egocentric test videos into INPUT_VIDEO_DIR from the demo bucket.

The ~24h city-walk set (Japan ~8h, USA ~8h, India ~8h across 5 clips) plus a 3-min smoke clip live in
an object-storage bucket as pre-encoded 720p `.webm`. Override the base URL with $VIDEO_BUCKET_URL.

Internal-demo use only: the source footage is under YouTube ToS — do not redistribute the videos or
the frames sampled from them.

Usage:
    python scripts/fetch_video.py                          # the whole built-in test set
    python scripts/fetch_video.py smoke_shibuya_3min       # just the 3-min smoke clip (live demo)
    python scripts/fetch_video.py 27Pv4Cg4EV4 BsiHD4m6_BU  # specific bucket keys
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

# Base URL of the demo bucket holding the built-in test videos (public-read). Override via env.
BUCKET_URL = os.environ.get(
    "VIDEO_BUCKET_URL", "https://storage.yandexcloud.net/e8-demo/robots-ego-video"
)

# Built-in test set as bucket keys (without .webm). The 3-min smoke clip is listed separately so it
# can be fetched on its own for the live-demo stage (see the setup skill's two-stage choreography).
CITY_WALKS = [
    "BsiHD4m6_BU",  # Tokyo, 9 districts, ~8h
    "27Pv4Cg4EV4",  # New York full city walk, ~8h
    "60Q5E0KZb38",  # Mumbai markets, ~2h40
    "qskdzPj39hE",  # New Delhi Paharganj, ~2h
    "8W4ZTX1z02E",  # Mumbai busy streets, ~1h35
    "7wBNtsgqNOI",  # New Delhi crowds, ~1h
    "Lteooc0BHtk",  # New Delhi streets, ~40m
]
SMOKE = "smoke_shibuya_3min"  # 3-min live-demo clip
DEFAULT_KEYS = CITY_WALKS + [SMOKE]


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "keys", nargs="*", help="bucket keys to fetch, without .webm (default: the whole built-in set)"
    )
    parser.add_argument(
        "--dir", default=os.environ.get("INPUT_VIDEO_DIR"), help="target dir (default: $INPUT_VIDEO_DIR)"
    )
    parser.add_argument(
        "--base-url", default=BUCKET_URL, help="bucket base URL (default: $VIDEO_BUCKET_URL)"
    )
    args = parser.parse_args()

    if not args.dir:
        parser.error("set --dir or the INPUT_VIDEO_DIR env var")
    out_dir = Path(args.dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    keys = args.keys or DEFAULT_KEYS
    base = args.base_url.rstrip("/")
    rc = 0
    for key in keys:
        key = key[:-5] if key.endswith(".webm") else key
        url = f"{base}/{key}.webm"
        dst = out_dir / f"{key}.webm"
        print(f"-> {dst}  <-  {url}", file=sys.stderr)
        # -f fail on HTTP errors, -S show errors, -L follow redirects, -C - resume a partial download.
        ret = subprocess.run(["curl", "-fSL", "-C", "-", "-o", str(dst), url]).returncode
        rc = rc or ret
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
