# Egocentric video → SAM3 → CVAT ("robots" demo)

Drop a long first-person video into a folder — the pipeline samples frames, segments them with a
**text prompt** (SAM3), and hands you ready-to-review pre-annotations in **CVAT**. Built for a
robotics audience: *"here is the inbox folder, put your 20-hour egocentric recording in it, we
process it for you."*

It reuses the whole SAM3→CVAT tail from [`../sam_cvat`](../sam_cvat); the only thing added on the
front is turning a long video into a deduplicated set of frames.

## Pipeline

```
stage=video    list_videos      folder INPUT_VIDEO_DIR      -> video
stage=sample   extract_frames   ffmpeg fps=SAMPLE_FPS       -> frames
stage=sample   dedup_frames     perceptual-hash dedup       -> local_images
stage=ingest   list_sam_config  SAM_TEXT_PROMPT             -> sam_config
stage=sam      sam_inference    SAM3 image-mode             -> sam_predictions
stage=sam      sam_to_cvat_xml                              -> sam_cvat_xml
stage=cvat     prepare_cvat_input / CVATStep / parse_cvat_annotations -> image__annotations
```

`local_images (image_id, image_path)` is exactly what the SAM→CVAT tail consumes, so everything from
`sam_inference` onward is identical to `sam_cvat` — `models.py` is a verbatim copy. The only thing the
video front adds is turning a long video into a deduplicated set of frames.

## Why sample at 1 fps then dedup (not "every frame", not 1/3 fps)

Walking POV footage is extremely redundant — you move ~1 m/s and consecutive frames are near
duplicates, so **1 fps and 1/3 fps are not meaningfully different** in what ends up annotated. The
real lever is content-awareness. So: extract at `SAMPLE_FPS=1` for good event coverage, then drop
near-duplicates with a perceptual hash (`PHASH_MAX_DISTANCE`, Hamming). A 24h source at 1 fps is
~86k frames; after dedup it collapses to a few thousand genuinely different frames — enough coverage
without flooding CVAT. Both are ordinary incremental datapipe stages: add another video and only its
frames are processed.

## Prerequisites

- **GPU** for SAM3 (`DEVICE` auto-selects `cuda:0`). SAM3 runs at the frame's native resolution — a
  FlashAttention-capable card (Ada/Ampere+) handles full 720p in ~5 GB. A non-FlashAttention
  (Pascal-class) card falls back to O(n²) math-attention and OOMs 8 GB on 720p; use a bigger/newer GPU
  or sample smaller frames.
- **`ffmpeg`** on `PATH` (frame extraction). Not required: the example falls back to the binary
  bundled by `imageio-ffmpeg` when no system `ffmpeg` is found.
- **SAM3 is a gated HuggingFace model** — accept the license on the SAM3 model page, create a token,
  set `HF_TOKEN` in `.env`. If the home dir is read-only, point `HF_HOME` at a writable path so the
  gated weights + token cache there.
- **CVAT** deployed at `CVAT_URL` (see [`../datapipe_cvat/simple_project`](../datapipe_cvat/simple_project/README.md)
  for a local Docker setup), a project created (`CVAT_PROJECT_ID`) with labels matching
  `CVAT_BOX_LABEL` / `CVAT_POLYGON_LABEL` (defaults `person_box` / `person_mask`).

## Get a video

```bash
# built-in ~24h set of busy IN/JP/US city walks (720p), into $INPUT_VIDEO_DIR:
python scripts/fetch_video.py --height 720
# or your own:
python scripts/fetch_video.py --dir videos "https://youtu.be/VIDEO_ID"
# or just a clip (needs ffmpeg):
python scripts/fetch_video.py --section 00:10:00-00:20:00 "https://youtu.be/VIDEO_ID"
```

`fetch_video.py` needs `yt-dlp` and, for many YouTube videos, a JS runtime to solve YouTube's
n-challenge — install **`deno`** (or `node`); the EJS solver script is auto-fetched via
`--remote-components ejs:github`. Videos that demand sign-in ("confirm you're not a bot") also need
browser cookies — the script passes `--cookies-from-browser chrome` by default (be logged into
YouTube in that browser; use `--cookies-from-browser ""` to disable).

Internal-demo only — downloading violates YouTube ToS; do not redistribute the videos or the frames.
For license-clean footage use stock (Pexels/Mixkit) or a dataset clip (EPIC-KITCHENS / Ego4D).

## Run

```bash
cp .env.example .env          # set DB_URL, HF_TOKEN, INPUT_VIDEO_DIR, CVAT_*, SAM_TEXT_PROMPT
uv sync
datapipe db create-all
datapipe run
```

Run a single stage: `datapipe step --labels stage=sample run`, `... stage=sam run`,
`... stage=cvat run`.

SAM3 runs at the frame's native resolution. **Field note (measured):** on a FlashAttention GPU
(Ada/Ampere+, 16 GB) full 1280×720 SAM3 peaks at only **~5.3 GB, ~0.27 s/frame**. A non-FlashAttention
(Pascal-class) 8 GB card OOMs on 720p — use a newer/bigger GPU, or reduce frame size upstream (e.g. an
`ffmpeg` `scale` filter in `extract_frames`).

## Debug UI (`datapipe api`)

`app.py` exposes a `DatapipeAPI`, so `datapipe api` (default `:8000`) serves the pipeline graph, table
browser, and per-stage run triggers. **Do not run `datapipe api` and a separate CLI `datapipe run`
against the same database at the same time** — both write run logs to the shared Postgres and collide
on `uq_run_log_seq` (the API's orphan-run reconciler adopts the live CLI run), which kills the run.
Either trigger runs from the UI, or stop the UI while a CLI run is in flight.

## Live demo

Drop a ~10s clip into `INPUT_VIDEO_DIR` and `datapipe run`: it is sampled, deduped, segmented, and a
new CVAT task with box+polygon pre-annotations appears within a few seconds. Change `SAM_TEXT_PROMPT`
(e.g. `person` → `car`) to show open-vocabulary segmentation without retraining.

## Annotate in CVAT

Open CVAT, find the task, review/fix the SAM pre-annotations, save, mark completed. Re-run the
pipeline to pull the reviewed annotations back into `image__annotations`.
