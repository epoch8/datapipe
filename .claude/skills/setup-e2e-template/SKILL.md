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
- **Where YOUR images go:** put them under `$DATAPIPE_E2E_DIR/images` (`DATAPIPE_E2E_DIR` defaults to `s3://datapipe-e2e`; the pipeline writes its own artifacts under `$DATAPIPE_E2E_DIR/datapipe`, a sibling — input and working dir don't overlap). Set `AWS_*`, `S3_ENDPOINT_URL`, and `S3_PUBLIC_URL` (browser-reachable — Label Studio loads images from it). Label Studio reaches S3 via its own `LABEL_STUDIO_S3_ENDPOINT_URL` (`minio:9000`), not `S3_ENDPOINT_URL`.

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
Skip if you have data: `uv run python scripts/seed_sample_data.py` downloads ~20 COCO images (10 cat/dog + 10 person keypoints; `--detection-limit`/`--keypoints-limit` to change) and uploads them to MinIO; then run §Run as-is.

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
- **No model after `train`, exit 0** → datapipe swallows step errors; check `detection_training_status`, not the exit code. Training auto-retries failed/raced attempts (`resume_config` resumes from `last` up to `max_attempts`), so a transient first-attempt checkpoint-sync hiccup recovers on its own.
- **Demo pre-annotations are empty** → the fallback `DETECTION_MODEL_CONFIG` is a smoke model (`yolo11n`, `input_size:[16,16]`, `score_threshold:0.01`) used until a trained model exists, so it detects almost nothing. Expected for the demo; for useful pre-annotations set a real model + `input_size`/`score_threshold`.
- **Training metrics ~0** → not a config bug: the trained model is `yolov8n` (imgsz 320, 30 ep), and the seed sample (~20 images) is simply too small to learn from. Real metrics need enough annotated data.
- **`cv_pipeliner` keypoint pre-annotation is broken** (pinned rev): inferencer drops keypoints, LS parser doesn't apply them → keypoint `train` needs real keypoint GT injected.
