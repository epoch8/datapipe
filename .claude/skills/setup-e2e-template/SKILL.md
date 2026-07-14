---
name: setup-e2e-template
description: >
  Use when setting up or running examples/e2e_template (image_detection or image_keypoints) —
  datapipe YOLO/Label Studio pipelines — or debugging their install, stages, training, or FiftyOne output.
---

# e2e_template (detection + keypoints YOLO/Label-Studio pipeline)

This skill = run the YOLO/Label-Studio detection/keypoints pipeline on YOUR images. The COCO seed sample is just a smoke-test; the real goal is your own images — set the knobs below first.

**Ask first — don't assume (only the unresolved):** demo or your own data? bring up the bundled `docker compose` services or reuse existing? **which Postgres + which database** for `DB_URL` — never point it at an existing DB without confirming; reuse an existing venv / `uv` env or create a fresh one? GPU? **annotate for real in Label Studio, or inject ready-made ground truth** (reuse existing labels / a labelled dataset) to skip the human `annotation` step? surface stage logs or run quiet? (per-tag scenario metrics live in a separate example → **setup-detection-tags**)

**How to work:** read the setup, then propose a short plan and get a go-ahead before touching anything. Prepare `.env` and **pause for the user to verify it** before running. Run each stage with its logs shown and, after each, say what you did and what changed — don't run the pipeline silently. If a stage fails and the cause isn't clear from the normal logs, re-run it with `datapipe --debug … run` (or `--debug-sql` for SQL errors); debug is very verbose, so send it to a file and `grep` it (e.g. `datapipe --debug run > /tmp/dp_debug.log 2>&1; grep -nEi "error|traceback" /tmp/dp_debug.log`) rather than dumping it inline.

## Run on YOUR data
- **Align your class everywhere** (mismatch → 0 useful results): `LABEL_CONFIG` names == `CLASSES_TO_KEEP`. Detection also `COCO_CLASSES`/`DETECTION_MODEL_CONFIG`; keypoints also `KEYPOINTS_LABELS` (order matters), `COCO_PERSON_KEYPOINT_FLIP_IDX`, `KEYPOINTS_MODEL_CONFIG`.
- **Where YOUR images go:** put them under `$DATAPIPE_E2E_DIR/images` (`DATAPIPE_E2E_DIR` defaults to `s3://datapipe-e2e`; the pipeline writes its own artifacts under `$DATAPIPE_E2E_DIR/datapipe`, a sibling — input and working dir don't overlap). Set `AWS_*`, `S3_ENDPOINT_URL`, and `S3_PUBLIC_URL` (browser-reachable — Label Studio loads images from it). Label Studio reaches S3 via its own `LABEL_STUDIO_S3_ENDPOINT_URL` (`minio:9000`), not `S3_ENDPOINT_URL`.

## Prerequisites
- **Python 3.10–3.12** (hard pin `>=3.10,<3.13`). **Install:** `cd examples/e2e_template && uv sync`
  (`pyproject.toml`: editable `datapipe-app`, **`datapipe-app-ml-ops`**, `datapipe-ml`, cu124 `torch==2.6.0`).
  Modern hosts: run `uv sync` unmodified — do NOT edit/re-lock deps (drifts training across machines).
  **Legacy host (pre-AVX2 CPU, e.g. epoch8 gpu5):** `uv sync --extra old-cpu`, then force lts polars
  (`uv pip uninstall polars polars-lts-cpu && uv pip install polars-lts-cpu==1.33.1`) — see detection_tags Troubleshooting.
- **Ops specs / app UI:** `image_*/app.py` registers specs via `datapipe_app_ml_ops.ops.ops_specs` +
  `app.add_specs([...])`. Run the front with `datapipe --pipeline app api` (port 8000) after `uv sync`.
- **Services:** `docker compose up` → Postgres 5432, MinIO (9000/9001, bucket `datapipe-e2e`,
  anon-download for browser images), MongoDB 27017, Label Studio :8080, ClickHouse :8123 (ops run logs).
- **Env:** `cp .env.example .env` then `set -a && source .env && set +a` before any `datapipe` command
  (`config.py` raises if `DB_URL` or `CLICKHOUSE_RUN_LOGS_URL` unset). Detection/keypoints use separate schemas.
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

## Skip annotation — inject ground truth (unattended / demo)
No human in Label Studio? You can bypass `annotation`/`ls-sync` by writing `image__ground_truth` +
`image__subset` directly, then run `stage=train`. **The injected rows MUST follow the pipeline's own
conventions, or the freeze join silently yields nothing / classes don't match:**
- **`image_name` must equal what `list_s3_images` emits** — the key **relative to `INPUT_IMAGES_DIR`
  (`$DATAPIPE_E2E_DIR/images`)**, i.e. the plain object name (`000000008458.jpg`). Don't invent a
  prefixed form — if `s3_images` has `X` and your GT has `pfx___X`, the `GT ⋈ subset ⋈ s3_images` join
  is empty → `freeze_dataset` fails with `No ground truth`.
- **`labels` must match `DETECTION_CLASSES` casing** (e.g. `Cat`/`Dog`, not COCO lowercase `cat`/`dog`)
  — otherwise predictions and GT land in *different* classes and every metric is 0.
- **`bboxes`** = pixel `[x1,y1,x2,y2]`; assign `image__subset.subset_id` (`train`/`val`) yourself.
- Write via datapipe `DataStore`/`UpdateExternalTable` (keeps `*_meta` in sync) — not raw SQL `UPDATE`
  on PK columns. Then `datapipe step --labels=stage=train run` (skip `annotation`/`ls-sync`).

## Per-scenario tag metrics → see the dedicated example
Want to tag a scenario (e.g. dark-room pallets), add it to training, and measure the model on that
scenario separately (baseline vs retrained)? That lives as its own self-contained example —
`examples/detection_tags` (`tag`/`image__tag`, `CountMetrics_Subset_PipelineModel` tag arc,
**no Label Studio**, ground truth injected, **FiftyOne** baseline vs retrained). Use the
**setup-detection-tags** skill.

## Troubleshooting (may already be fixed — verify against current files)
- **No model after `train`, exit 0** → datapipe swallows step errors; check `detection_training_status`, not the exit code.
- **Demo pre-annotations are empty** → the fallback `DETECTION_MODEL_CONFIG` is a smoke model (`yolo11n`, `input_size:[16,16]`, `score_threshold:0.01`) used until a trained model exists, so it detects almost nothing. Expected for the demo; for useful pre-annotations set a real model + `input_size`/`score_threshold`.
- **Training metrics ~0** → not a config bug: the trained model is `yolov8n` (imgsz 320, 30 ep), and the seed sample (~20 images) is simply too small to learn from. Real metrics need enough annotated data.
- **`SIGILL` / pre-AVX2 CPU** → same as **setup-detection-tags**: `uv sync --extra old-cpu`, reinstall
  `polars-lts-cpu==1.33.1`, and `pi-heif` if ultralytics image verification fails.
- **`cv_pipeliner` keypoint pre-annotation is broken** (pinned rev): inferencer drops keypoints, LS parser doesn't apply them → keypoint `train` needs real keypoint GT injected.
