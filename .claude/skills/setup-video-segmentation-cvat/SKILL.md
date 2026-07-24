---
name: setup-video-segmentation-cvat
description: >
  Use when working in examples/video_segmentation_cvat, or when setting up / running / debugging the
  datapipe video → SAM3 → CVAT example (long egocentric video → sampled frames → text-prompt
  segmentation → CVAT pre-annotations), on the built-in city-walk videos or your own footage.
---

# video_segmentation_cvat (video → SAM3 → CVAT)

This skill = turn a long video into review-ready CVAT pre-annotations. Front stage samples + dedups
frames; the SAM3→CVAT tail is identical to [`../sam_cvat`](../sam_cvat). Set the knobs below first.

**Ask first — don't assume (only the unresolved):** which videos (built-in city-walk set via
`scripts/fetch_video.py`, or the user's own) → `INPUT_VIDEO_DIR`? **`SAMPLE_FPS`** (how densely to
sample — the single biggest lever on frame count / run time) and **`SEGMENT_SIZE`** (frames per CVAT
job)? **which Postgres + which database** for `DB_URL` — never point at an existing DB or use a
default without confirming; external CVAT ready or provision it? reuse an existing venv / `uv` env or
create fresh? which GPU (VRAM + FlashAttention)? surface stage logs or run quiet?

**How to work:** read the setup, propose a short plan, get a go-ahead before touching anything.
Prepare `.env` and **pause for the user to verify it** before running. Run stages with logs shown;
after each, say what you did and what changed — don't run silently. If a stage fails and the cause
isn't clear, re-run with `datapipe --debug … run` (`--debug-sql` for SQL); debug is very verbose, so
send it to a file and `grep` (`datapipe --debug run > /tmp/dp.log 2>&1; grep -nEi "error|traceback" /tmp/dp.log`).

## Pipeline
```
stage=video   list_videos      folder INPUT_VIDEO_DIR  -> video
stage=sample  extract_frames   ffmpeg fps=SAMPLE_FPS   -> frames
stage=sample  dedup_frames     perceptual-hash dedup   -> local_images
stage=ingest  list_sam_config  SAM_TEXT_PROMPT         -> sam_config
stage=sam     sam_inference    SAM3 image-mode         -> sam_predictions
stage=sam     sam_to_cvat_xml                          -> sam_cvat_xml
stage=cvat    prepare_cvat_input / CVATStep / parse_cvat_annotations -> image__annotations
```
`local_images (video_id, image_id, image_path)` is what the SAM→CVAT tail consumes; everything from
`sam_inference` on (incl. `models.py`) is a **verbatim copy of `sam_cvat`** — keep it that way. The
video front's only job is video → deduped frames (SAM runs at native resolution). **One CVAT task per
video** (`task_queue_id=video_id`, `FILES_BATCH` huge), split into jobs of `SEGMENT_SIZE` frames.

## Sampling knobs (the front)
- **`SAMPLE_FPS`** (default 0.2 = 1 frame/5 s). Walking POV is highly redundant — 1 fps vs 1/3 fps
  barely differ in what gets annotated; sample sparser to cut annotation load. **Ask before a run.**
- **`PHASH_MAX_DISTANCE`** (Hamming, default 10) — higher = more aggressive near-duplicate dedup.
- **Gotcha:** `extract_frames` reuses frames already in `FRAMES_DIR`, so **changing `SAMPLE_FPS` has
  no effect until you clear `FRAMES_DIR`** (`rm -rf <FRAMES_DIR>/<video_id>`).
- Align class across all three: `SAM_TEXT_PROMPT` == `CVAT_BOX_LABEL`/`CVAT_POLYGON_LABEL` — mismatch
  runs clean but yields 0 useful annotations. Levers: `SAM_SCORE_THRESHOLD` (0.5), `SAM_MAX_DETECTIONS` (20).

## GPU
SAM3 runs at the frame's **native resolution** (no in-pipeline resize). *Field note (measured):* a
FlashAttention GPU (Ada/Ampere+, 16 GB) does full 1280×720 in **~5.3 GB, ~0.27 s/frame**. A
non-FlashAttention (Pascal-class) 8 GB card OOMs on raw 720p — use a newer/bigger GPU, or shrink
frames upstream (e.g. add a `scale` filter to `extract_frames`) rather than editing the shared tail.

## Prerequisites
- **GPU** (see above). **HF token** — SAM3 is gated: accept the license at huggingface.co/facebook/sam3,
  set `HF_TOKEN` (gated read access). **Read-only home?** set `HF_HOME=/writable/path` so weights +
  token cache there (the example already avoids persisting the token to disk when `HF_TOKEN` is set).
- **`ffmpeg`** on `PATH`, else the `imageio-ffmpeg` bundled binary is used automatically.
- **External PostgreSQL** at `DB_URL` (tables auto-create via `datapipe db create-all`).
- **External CVAT** at `CVAT_URL` (user/pass, optional org), project `CVAT_PROJECT_ID` whose labels
  match `CVAT_BOX_LABEL` / `CVAT_POLYGON_LABEL` (defaults `person_box` / `person_mask`).
- **`uv` + Python ≥3.10,<3.13** → `uv sync`. Pins cu124 torch, editable local libs
  (`../../libs/datapipe-*`, monorepo-only), builds `sam3` from a pinned git rev (+`imagehash`). After
  `uv sync`, on a pre-AVX2 host re-apply `uv pip install polars-lts-cpu==1.33.1`.

## Get a video
```bash
python scripts/fetch_video.py --height 720                       # built-in ~24h city-walk set
python scripts/fetch_video.py --dir videos "https://youtu.be/ID" # your own (needs yt-dlp + deno)
python scripts/fetch_video.py --section 00:10:00-00:20:00 "https://youtu.be/ID"  # just a clip
```
Needs `yt-dlp` + a JS runtime (`deno`/`node`) for YouTube's n-challenge; sign-in-gated videos need
`--cookies-from-browser chrome`. Internal-demo only (YouTube ToS) — don't redistribute.

## Run
```bash
cp .env.example .env    # DB_URL, HF_TOKEN, INPUT_VIDEO_DIR, CVAT_*, SAM_TEXT_PROMPT, SAMPLE_FPS, SEGMENT_SIZE
uv sync && source .venv/bin/activate    # else prefix each command with `uv run`
datapipe db create-all && datapipe run
# by stage: datapipe step --labels stage=sample run  (then stage=sam, stage=cvat)
```
Run from `examples/video_segmentation_cvat/` (`app.py` `load_dotenv()`s before importing config).
**Live demo:** drop a ~10 s clip in `INPUT_VIDEO_DIR` → `datapipe run` → a CVAT task appears in seconds.

## Deploy on a bare GPU pod (no Docker) — field-tested
The example itself needs **no Docker** — it's a plain `uv` venv. Docker is only for the infra deps
(Postgres + CVAT), so on a Docker-less pod either point `DB_URL`/`CVAT_URL` at a host that runs them
(needs network reach to their ports) or, for a SAM-only smoke test, skip them.

Setup that worked, in order:
1. **Get the code on the branch** without disturbing an existing checkout: from the repo dir,
   `git fetch origin <branch>` then `git worktree add <path> <branch>` (reuses the host's git creds,
   isolated working tree).
2. **`uv sync`** in the example dir. `uv` auto-fetches a Python 3.10–3.12 even if the base interpreter
   is older. Takes a few min (torch cu124 ~2.5 GB + builds `sam3`/`cv-pipeliner` from git). **`sam3`
   builds with no `nvcc`/CUDA toolkit** — it's pure PyTorch, no compiled CUDA ext (the usual build
   worry is a non-issue).
3. **`.env`** — minimum for a SAM run: `HF_TOKEN` (gated), `HF_HOME=/writable/path` (the default home
   cache may be small/read-only; weights are ~3.3 GB), `SAM_TEXT_PROMPT`, and **a `DB_URL` even if
   unused** — `config.py` builds `DBConn(DB_URL)` at import and crashes on `None` (a dummy
   `postgresql+psycopg2://x:x@localhost:5432/x` is enough when you only call `infer_image`).
4. **Run scripts from the example dir** (or set `PYTHONPATH` to it) — `import models`/`config` are
   top-level modules, not a package.

**GPU field note (measured):** on a FlashAttention GPU (Ada/Ampere+, 16 GB), **full 1280×720 SAM3 =
~5.3 GB peak, ~0.27 s/frame** — comfortable, ~10× faster than a non-FA card. First run pays a one-time
~250 s model load + weight download.

## Debug UI (`datapipe api`)
`datapipe api` (default `:8000`) serves the pipeline graph, table browser, and per-stage run triggers.
**Do not run `datapipe api` and a separate CLI `datapipe run` on the same DB at once** — both write
run logs to the shared Postgres and collide on `uq_run_log_seq` (the API's orphan-run reconciler
adopts the live CLI run), which kills the run. Either trigger runs from the UI, or stop the UI while a
CLI run is in flight. (Radical fix: give the API a dedicated ClickHouse `RunLogsBackend`.)

## Annotate (human-in-the-loop)
In CVAT fix the pre-annotations, **mark the task completed**, re-run `datapipe run` → edits land in
`image__annotations`. CVAT tables (for wipes): `image_batches`, `cvat_task`, `cvat_images`,
`cvat_task_sync_table`. `CVATStep` does NOT push new pre-annotations to EXISTING tasks (only on image
add/remove/path change) — to change `SAM_TEXT_PROMPT`, wipe old tasks + datapipe CVAT tables, re-run.

## Troubleshooting (may already be fixed — verify against current files)
- **0 detections** → class misaligned; align `SAM_TEXT_PROMPT` with the CVAT labels.
- **OOM** → non-FlashAttention / too-small GPU for native-res SAM3; use a bigger/newer card or shrink
  frames upstream (`scale` filter in `extract_frames`).
- **Changing `SAMPLE_FPS` did nothing** → clear `FRAMES_DIR` (frames are reused).
- **`uq_run_log_seq` duplicate key** → UI + CLI ran together; run one at a time.
- **Read-only filesystem on HF cache** → set `HF_HOME` to a writable path.
- **CVAT rejects label** → project needs labels named `CVAT_BOX_LABEL`/`CVAT_POLYGON_LABEL`.
