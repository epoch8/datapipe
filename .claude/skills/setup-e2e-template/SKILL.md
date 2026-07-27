---
name: setup-e2e-template
description: >
  Use when setting up or running examples/e2e_template (image_detection, image_keypoints, or
  image_classification) — datapipe YOLO/TF + Label Studio pipelines — or debugging their install,
  stages, training, or FiftyOne output.
---

# e2e_template (detection + keypoints + classification)

This skill = run the Label-Studio e2e pipelines on YOUR images. The COCO seed sample is just a
smoke-test; the real goal is your own images — set the knobs below first.

Templates:
- `image_detection` — YOLO bbox (Cat/Dog)
- `image_keypoints` — YOLO pose (Person)
- `image_classification` — TF classification (`Has Animal` / `No Animals`)

**Ask first — don't assume (only the unresolved):** demo or your own data? which template?
bring up the bundled `docker compose` services or reuse existing? **which Postgres + which
database** for `DB_URL` — never point it at an existing DB without confirming; reuse an existing
venv / `uv` env or create a fresh one? GPU? **annotate for real in Label Studio, or inject
ready-made ground truth** (reuse existing labels / a labelled dataset) to skip the human
`annotation` step? surface stage logs or run quiet? (per-tag scenario metrics live in a
separate example → **setup-detection-tags**)

**How to work:** read the setup, then propose a short plan and get a go-ahead before touching
anything. Prepare `.env` and **pause for the user to verify it** before running. Run each stage
with its logs shown and, after each, say what you did and what changed — don't run the pipeline
silently. If a stage fails and the cause isn't clear from the normal logs, re-run it with
`datapipe --debug … run` (or `--debug-sql` for SQL errors); debug is very verbose, so send it to
a file and `grep` it (e.g. `datapipe --debug run > /tmp/dp_debug.log 2>&1; grep -nEi
"error|traceback" /tmp/dp_debug.log`) rather than dumping it inline.

## Run on YOUR data
- **Align your class everywhere** (mismatch → 0 useful results): `LABEL_CONFIG` names == class
  constants. Detection: `CLASSES_TO_KEEP` + `COCO_CLASSES`/`DETECTION_MODEL_CONFIG`. Keypoints:
  `KEYPOINTS_LABELS` (order matters), `COCO_PERSON_KEYPOINT_FLIP_IDX`, `KEYPOINTS_MODEL_CONFIG`.
  Classification: `CLASSIFICATION_CLASSES` = `Has Animal` / `No Animals` (Choices, not rectangles).
- **Where YOUR images go:** put them under `$DATAPIPE_E2E_DIR/images` (`DATAPIPE_E2E_DIR` defaults
  to `s3://datapipe-e2e`; the pipeline writes its own artifacts under `$DATAPIPE_E2E_DIR/datapipe`,
  a sibling — input and working dir don't overlap). Set `AWS_*`, `S3_ENDPOINT_URL`, and
  `S3_PUBLIC_URL` (browser-reachable — Label Studio loads images from it). Label Studio reaches S3
  via its own `LABEL_STUDIO_S3_ENDPOINT_URL` (`minio:9000`), not `S3_ENDPOINT_URL`.

## Prerequisites
- **Python 3.10–3.12** (hard pin `>=3.10,<3.13`). **Install:** `cd examples/e2e_template && uv sync --extra ray`
  (`pyproject.toml`: editable `datapipe-app`, **`datapipe-app-ml-ops`**, `datapipe-ml[torch,fiftyone,tensorflow]`,
  cu124 `torch==2.6.0`).
  Modern hosts: run `uv sync --extra ray` unmodified — do NOT edit/re-lock deps (drifts training across machines).
  **Legacy host (pre-AVX2 CPU, e.g. epoch8 gpu5):** `uv sync --extra ray --extra old-cpu`, then force lts polars
  (`uv pip uninstall polars polars-lts-cpu && uv pip install polars-lts-cpu==1.33.1`) — see detection_tags Troubleshooting.
- **Ops specs / app UI:** `image_*/app.py` registers specs via `datapipe_app_ml_ops.ops.ops_specs` +
  `app.add_specs([...])`. Run the front with `uv run datapipe --executor RayExecutor --pipeline app api`
  after `uv sync --extra ray` (ports: detection 8001, keypoints 8002, classification 8003).
- **Services:** `docker compose up` → Postgres 5432, MinIO (9000/9001, bucket `datapipe-e2e`,
  anon-download for browser images), MongoDB 27017, Label Studio :8080, ClickHouse :8123 (ops run logs).
- **Env:** `cp .env.example .env` then `set -a && source .env && set +a` before any `datapipe` command
  (`config.py` raises if `DB_URL` or `CLICKHOUSE_RUN_LOGS_URL` unset). Separate schemas:
  `DB_SCHEMA_DETECTION`, `DB_SCHEMA_KEYPOINTS`, `DB_SCHEMA_CLASSIFICATION`.
- **LS API token:** :8080 → Account & Settings → Access Token → `LABEL_STUDIO_API_KEY=...` in `.env`
  (or `scripts/label_studio_token.py`).
- **GPU for training.** YOLO has no CPU knob. TF classification needs GPU **or**
  `CUDA_VISIBLE_DEVICES=` (empty) so `cpu_training_allowed` passes. **≥10 annotated images** to
  freeze (`min_delta=10`).
- Read-only/small `/home`: `export UV_CACHE_DIR=/tmp/uvcache HF_HOME=/tmp/hf` before `uv sync`.
- Stages: `annotation`, `train`, `fiftyone`.

## Quick demo to verify setup
Skip if you have data: `uv run python scripts/seed_sample_data.py` downloads COCO images (cat/dog,
person keypoints; limit flags to change) and uploads them to MinIO; then run §Run as-is.
For classification add `--classification-animal-limit 12 --classification-no-animal-limit 12`
to seed mixed Has Animal / No Animals images for Label Studio.

## Run (from the project subdir)
```bash
cd image_detection                            # or image_keypoints / image_classification
set -a && source ../.env && set +a            # config raises w/o DB_URL
uv run datapipe db create-all
uv run datapipe --executor RayExecutor step --labels=stage=annotation run   # LS tasks (+ YOLO pre-anns for det/kp)
# → annotate ≥10 images in LS (:8080), mark completed →
uv run datapipe --executor RayExecutor step --labels=stage=annotation run   # sync annotations → image__ground_truth
uv run datapipe --executor RayExecutor step --labels=stage=train run
uv run datapipe --executor RayExecutor step --labels=stage=fiftyone run
uv run fiftyone app launch datapipe_detection_e2e    # or datapipe_keypoints_e2e / datapipe_classification_e2e
```

## Skip annotation — inject ground truth (unattended / demo)
No human in Label Studio? Bypass `annotation` by writing `image__ground_truth` + `image__subset`,
then run `stage=train`. **Rows MUST follow the pipeline's conventions:**
- **`image_name` must equal what `list_s3_images` emits** — key relative to `INPUT_IMAGES_DIR`
  (`$DATAPIPE_E2E_DIR/images`), plain object name (`000000008458.jpg`).
- **Detection/keypoints:** `labels` match class casing; `bboxes` = pixel `[x1,y1,x2,y2]`.
- **Classification:** scalar `label` column (`Has Animal` / `No Animals`) — not a JSON list.
- Write via datapipe `DataStore`/`UpdateExternalTable` (keeps `*_meta` in sync) — not raw SQL
  `UPDATE` on PK columns.

## Per-scenario tag metrics → see the dedicated example
Want to tag a scenario (e.g. dark-room pallets), add it to training, and measure the model on that
scenario separately (baseline vs retrained)? That lives as its own self-contained example —
`examples/detection_tags`. Use the **setup-detection-tags** skill.

## Troubleshooting (may already be fixed — verify against current files)
- **No model after `train`, exit 0** → datapipe swallows step errors; check `*_training_status`, not the exit code.
- **TF train fails with "GPU not found"** → set `CUDA_VISIBLE_DEVICES=` (empty) for CPU, or use a real GPU.
- **Demo detection pre-annotations are empty** → fallback `DETECTION_MODEL_CONFIG` is a smoke model until a trained model exists.
- **Training metrics ~0** → seed sample is small; real metrics need enough annotated data.
- **`SIGILL` / pre-AVX2 CPU** → same as **setup-detection-tags**: `uv sync --extra ray --extra old-cpu`, reinstall
  `polars-lts-cpu==1.33.1`, and `pi-heif` if ultralytics image verification fails.
- **`cv_pipeliner` keypoint pre-annotation is broken** (pinned rev): inferencer drops keypoints, LS parser doesn't apply them → keypoint `train` needs real keypoint GT injected.
