from __future__ import annotations

import os
from pathlib import Path

import torch
from datapipe.store.database import DBConn

# --- video ingest / frame sampling ---------------------------------------------------------------

# Folder you drop long egocentric videos into. Every video in here is processed.
_INPUT_VIDEO_DIR_RAW = os.environ.get("INPUT_VIDEO_DIR")
INPUT_VIDEO_DIR = Path(_INPUT_VIDEO_DIR_RAW).resolve() if _INPUT_VIDEO_DIR_RAW else None
VIDEO_SUFFIXES = {".mp4", ".mkv", ".mov", ".webm", ".m4v"}

# Where extracted frames are written (one subfolder per video_id).
_FRAMES_DIR_RAW = os.environ.get("FRAMES_DIR")
FRAMES_DIR = (
    Path(_FRAMES_DIR_RAW).resolve()
    if _FRAMES_DIR_RAW
    else Path(__file__).resolve().parent / ".frames"
)

# ffmpeg extraction rate. 1 fps gives good event coverage; 1/3fps is not meaningfully different for
# walking POV footage (see README). Dedup below removes the near-duplicates either way.
SAMPLE_FPS = float(os.environ.get("SAMPLE_FPS", "1"))


def _resolve_ffmpeg() -> str:
    # Prefer a system ffmpeg; fall back to the binary bundled by the imageio-ffmpeg package so the
    # example runs on hosts without ffmpeg on PATH.
    import shutil

    exe = os.environ.get("FFMPEG_BIN") or shutil.which("ffmpeg")
    if exe:
        return exe
    try:
        import imageio_ffmpeg

        return imageio_ffmpeg.get_ffmpeg_exe()
    except Exception:
        return "ffmpeg"


FFMPEG_BIN = _resolve_ffmpeg()

# Perceptual-hash near-duplicate threshold (Hamming distance between consecutive frames). A frame is
# kept only if it differs from the last kept frame by more than this. Higher = more aggressive dedup.
PHASH_MAX_DISTANCE = int(os.environ.get("PHASH_MAX_DISTANCE", "10"))
PHASH_SIZE = int(os.environ.get("PHASH_SIZE", "8"))  # phash hash_size (bits per side)

IMAGE_SUFFIXES = {".jpg", ".jpeg", ".png"}

# --- SAM3 -----------------------------------------------------------------------------------------

DEVICE = "cuda:0" if torch.cuda.is_available() else "cpu"

HF_TOKEN = os.environ.get("HF_TOKEN", "")
SAM_TEXT_PROMPT = os.environ.get("SAM_TEXT_PROMPT", "person")
SAM_SCORE_THRESHOLD = float(os.environ.get("SAM_SCORE_THRESHOLD", "0.5"))
SAM_MAX_DETECTIONS = int(os.environ.get("SAM_MAX_DETECTIONS", "20"))

# --- CVAT -----------------------------------------------------------------------------------------

TASK_QUEUE_ID = os.environ.get("TASK_QUEUE_ID", "queue1")
FILES_BATCH = int(os.environ.get("FILES_BATCH", "500"))

CVAT_URL = os.environ.get("CVAT_URL", "http://localhost:8080")
CVAT_USERNAME = os.environ.get("CVAT_USERNAME", "admin")
CVAT_PASSWORD = os.environ.get("CVAT_PASSWORD", "admin")
CVAT_PROJECT_ID = int(os.environ.get("CVAT_PROJECT_ID", "1"))
CVAT_ORGANIZATION = os.environ.get("CVAT_ORGANIZATION", "")
CVAT_PROJECT_NAME = os.environ.get("CVAT_PROJECT_NAME", "datapipe-robots-ego-cvat")

CVAT_BOX_LABEL = os.environ.get("CVAT_BOX_LABEL", "person_box")
CVAT_POLYGON_LABEL = os.environ.get("CVAT_POLYGON_LABEL", "person_mask")

PRIMARY_KEYS = ["image_id", "task_queue_id"]

# --- datapipe -------------------------------------------------------------------------------------

DB_URL = os.environ.get("DB_URL")
DBCONN = DBConn(DB_URL, None)
