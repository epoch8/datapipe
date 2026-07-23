#!/usr/bin/env python3
"""Download egocentric source videos into INPUT_VIDEO_DIR with yt-dlp.

Internal-demo use only: downloading violates YouTube ToS, so do not redistribute the videos or the
frames sampled from them. For license-clean footage use stock (Pexels/Mixkit) or a dataset clip.

Usage:
    python scripts/fetch_video.py                       # the default ~24h walk set (IN/JP/US), 720p
    python scripts/fetch_video.py --height 480          # smaller download
    python scripts/fetch_video.py URL [URL ...]         # your own videos
    python scripts/fetch_video.py --section 00:10:00-00:20:00 URL   # a clip only (needs ffmpeg)
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

# Verified (yt-dlp) ~24h of busy first-person city walks: Japan 8h, USA 8h, India ~8h (5 clips).
DEFAULT_VIDEOS = [
    "https://youtu.be/BsiHD4m6_BU",  # Tokyo, 9 districts, 8:06:59, 4K
    "https://youtu.be/27Pv4Cg4EV4",  # New York full city walk, 8:04:31, 4K
    "https://youtu.be/60Q5E0KZb38",  # Mumbai markets, 2:41:51, 4K
    "https://youtu.be/qskdzPj39hE",  # New Delhi Paharganj, 1:57:15, 4K
    "https://youtu.be/8W4ZTX1z02E",  # Mumbai busy streets, 1:36:11, 4K
    "https://youtu.be/7wBNtsgqNOI",  # New Delhi crowds, 0:58:59, 4K
    "https://youtu.be/Lteooc0BHtk",  # New Delhi streets, 0:39:41, 4K
]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("urls", nargs="*", help="video URLs (default: the built-in ~24h walk set)")
    parser.add_argument("--dir", default=os.environ.get("INPUT_VIDEO_DIR"), help="target dir (default: $INPUT_VIDEO_DIR)")
    parser.add_argument("--height", type=int, default=720, help="max video height, e.g. 480/720/1080 (default 720)")
    parser.add_argument("--section", default=None, help="download only a section, e.g. 00:10:00-00:20:00")
    parser.add_argument("--cookies-from-browser", default="chrome",
                        help="browser to read cookies from (chrome/safari/firefox/...); '' to disable")
    args = parser.parse_args()

    if not args.dir:
        parser.error("set --dir or the INPUT_VIDEO_DIR env var")
    out_dir = Path(args.dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    urls = args.urls or DEFAULT_VIDEOS
    fmt = f"bv*[height<={args.height}]+ba/b[height<={args.height}]/b[height<={args.height}]"
    cmd = [
        "yt-dlp",
        "-N", "4",
        "--retries", "infinite",
        "--fragment-retries", "infinite",
        "--sleep-interval", "3", "--max-sleep-interval", "12",
        # Some videos require sign-in ("confirm you're not a bot") -> pass browser cookies. Solving
        # YouTube's JS n-challenge needs a JS runtime (install `deno` or `node`) plus the EJS solver
        # script, fetched by --remote-components; without it only storyboard images are returned.
        "--remote-components", "ejs:github",
        "-f", fmt,
        "-o", str(out_dir / "%(id)s.%(ext)s"),
    ]
    if args.cookies_from_browser:
        cmd += ["--cookies-from-browser", args.cookies_from_browser]
    if args.section:
        cmd += ["--download-sections", f"*{args.section}", "--force-keyframes-at-cuts"]
    cmd += urls

    print(f"Downloading {len(urls)} video(s) -> {out_dir} (<= {args.height}p)", file=sys.stderr)
    return subprocess.run(cmd).returncode


if __name__ == "__main__":
    raise SystemExit(main())
