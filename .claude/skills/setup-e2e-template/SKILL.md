---
name: setup-e2e-template
description: >
  Use when setting up or running examples/e2e_template (image_detection or image_keypoints) —
  datapipe YOLO/Label Studio pipelines — or debugging their install, stages, training, or FiftyOne output.
---

# e2e_template (detection + keypoints YOLO/Label-Studio pipeline)

This skill = run the YOLO/Label-Studio detection/keypoints pipeline on YOUR images. The COCO seed sample is just a smoke-test; the real goal is your own images — set the knobs below first.

**Before starting, if not already provided, ask the user:** demo or your own data? bring up the `docker compose` services or reuse existing? GPU available? per-tag scenario metrics ([tags-addon.md](tags-addon.md))? — ask only what's unresolved, then do only what's needed.

## Run on YOUR data
- **Align your class everywhere** (mismatch → 0 useful results): `LABEL_CONFIG` names == `CLASSES_TO_KEEP`. Detection also `COCO_CLASSES`/`DETECTION_MODEL_CONFIG`; keypoints also `KEYPOINTS_LABELS` (order matters), `COCO_PERSON_KEYPOINT_FLIP_IDX`, `KEYPOINTS_MODEL_CONFIG`.
- **Working-dir overlap (fix this):** shipped `S3_PREFIX=images` + `S3_DATAPIPE_PREFIX=images/datapipe` is nested inside the input, and `list_s3_images` recurses `{bucket}/images` → pipeline re-ingests its own outputs. Set `S3_DATAPIPE_PREFIX` (or `DATAPIPE_E2E_DIR`) **outside** `S3_PREFIX`.
- **Own S3 (where YOUR images go):** set `AWS_*`, `S3_BUCKET`, `S3_PREFIX`, `S3_PUBLIC_URL` (browser-reachable; LS loads from `$S3_PUBLIC_URL/$S3_BUCKET/<key>`). Two endpoints: `S3_ENDPOINT_URL` (host) vs `LABEL_STUDIO_S3_ENDPOINT_URL` (`minio:9000`).

## Prerequisites
- **Python 3.10–3.12** (hard pin `>=3.10,<3.13`). **Install:** `cd examples/e2e_template && uv sync`
  (`pyproject.toml` declares editable workspace libs + cu124 `torch==2.6.0`).
- **Services:** `docker compose up` → Postgres 5432, MinIO (9000/9001, bucket `datapipe-e2e`,
  anon-download for browser images), MongoDB 27017, Label Studio :8080.
- **Env:** `cp .env.example .env` then `set -a && source .env && set +a` before any `datapipe` command
  (`config.py` raises if `DB_URL` unset). Detection/keypoints use separate schemas.
- **LS API token:** :8080 → Account & Settings → Access Token → `LABEL_STUDIO_API_KEY=...` in `.env`
  (or `scripts/label_studio_token.py`).
- **GPU for training** (no CPU knob). **≥10 annotated images** to freeze a dataset (`min_delta=10`).
- Read-only/small `/home`: `export UV_CACHE_DIR=/tmp/uvcache HF_HOME=/tmp/hf` before `uv sync`.
- Stages: `annotation`, `ls-sync`, `train`, `fiftyone`.

## Quick demo to verify setup
Skip if you have data: `uv run python scripts/seed_sample_data.py` makes schemas, caches COCO (~241MB, override `DATAPIPE_CACHE_DIR`), uploads ~20 JPEGs to MinIO; then run §Run as-is.

## Run (from the project subdir)
```bash
cd image_detection                            # or image_keypoints
source ../.venv/bin/activate                  # else prefix every command with `uv run`
set -a && source ../.env && set +a            # config raises w/o DB_URL
datapipe db create-all
datapipe step --labels=stage=annotation run   # LS tasks + pre-annotations
# → annotate ≥10 images in LS (:8080), mark completed →
datapipe step --labels=stage=ls-sync run      # → image__ground_truth
datapipe step --labels=stage=train run        # freeze + train YOLO + metrics + best
datapipe step --labels=stage=fiftyone run
fiftyone app launch datapipe_detection_e2e    # or datapipe_keypoints_e2e
```
Train uses `yolov8n*.pt` (imgsz 320, 30 ep); pre-annotation fallback `yolo11n*.pt`; best on `subset_id=val`.

## Add-on upgrade — tags for per-scenario metrics
An **optional layer on top of the base pipeline** (not needed to run it). New case (e.g. dark-room
pallets) → tag those images, let part flow into training, and measure the model **on that scenario
separately** (old vs new) without touching the split. Bolt-on recipe (catalog `tag`/`image__tag` +
one `tag_metrics` step; example logic unchanged): [tags-addon.md](tags-addon.md). Verified end-to-end —
`tag_metrics` shows the retrained model gaining recall on the tagged scenario.

## Troubleshooting (may already be fixed — verify against current files)
- **Exit 0 but no model trained** → datapipe swallows step errors; check the `*_training_status` table, not the exit code.
- **First `train` fails: `No weight file found for best epoch N`** → S3 sync/rename race (`best.pt` still `.tmp`). Just **re-run** `stage=train` — it finalizes from the synced checkpoint. (keypoints avoid it via `save_period:1`.)
- **`detection` metrics all 0 / no best model** → the shipped `DETECTION_MODEL_CONFIG` is a smoke config (`input_size:[16,16]`, `score_threshold:0.01`) → model predicts nothing. For real metrics use `input_size:[320,320]`+`score_threshold:~0.25` and enough data.
- **Dataset silently grows** → working-dir nested in input prefix (see "Working-dir overlap").
- **`cv_pipeliner` keypoint pre-annotation is broken** (pinned rev): inferencer drops keypoints, LS parser doesn't apply them → keypoint `train` needs real keypoint GT injected.
- **Offline / restricted-network / pre-AVX2 nodes:** the shipped `polars-lts-cpu` pin breaks on pre-AVX2 (`ImportError`, mixed runtimes) → `uv pip install "polars[rtcompat]==1.42.0"` (the `rtcompat` extra; bare `polars-runtime-compat` isn't directly installable). AMP-check hangs downloading `yolo26n.pt` (pre-place it from the **`v8.4.0`** assets tag + the YOLO weights). `/home` read-only → `UV_*`/`UV_UNMANAGED_INSTALL` on `/tmp`. `seed_sample_data.py` aborts on a single COCO timeout on a flaky link → per-image retry.
