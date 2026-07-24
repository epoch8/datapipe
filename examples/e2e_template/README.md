# Datapipe E2E Template

**Claude Code skill:** `/setup-e2e-template` — auto-loads when relevant, or invoke it directly. It
carries the prerequisites, env knobs, and gotchas below.

This example contains end-to-end Datapipe pipeline templates:

- `image_detection` for bbox detection.
- `image_keypoints` for bbox + keypoints pose pipelines.
- `image_classification` for image-level classification (`Has Animal` / `No Animals`).

All templates cover the same lifecycle:

1. Load images from S3-compatible storage.
2. Create/sync tasks and predictions in Label Studio.
3. Parse annotations back into Datapipe tables (or inject COCO seed labels for classification).
4. Freeze a training dataset, train a model, run inference, and compute metrics.
5. Optionally publish predictions and annotations into FiftyOne.

Each template follows the same layout as a standalone project:

- `config.py` — environment variables and paths.
- `data.py` — `catalog` with input tables.
- `steps.py` — batch functions used by the pipeline.
- `app.py` — `pipeline`, `ds`, and `app` defined at module level.

Pipeline metadata is stored in Postgres. Set `DB_URL` in `.env` before running datapipe commands.
Detection, keypoints, and classification use separate Postgres schemas (`DB_SCHEMA_DETECTION`,
`DB_SCHEMA_KEYPOINTS`, `DB_SCHEMA_CLASSIFICATION`) so their tables do not collide when pipelines
share the same database.

All commands below assume the current directory is `examples/e2e_template/`. Use `uv run` for Python CLI tools (`datapipe`, `fiftyone`, scripts) so they run in the project virtualenv from `uv sync`.

## Installation

Python **3.10–3.12** is required (`datapipe-label-studio` and `datapipe-ml` do not support 3.13+ yet).

### Python packages

Dependencies live in `pyproject.toml`. Install with [uv](https://docs.astral.sh/uv/):

```bash
cd examples/e2e_template

uv sync --extra ray
```

Pipeline steps use `RayExecutor` for parallel I/O, inference, and metrics (see `executor_config` in
each template's `app.py`). Install the `ray` extra before running `datapipe step` or `datapipe api`.

On CPUs **without AVX2+** (ultralytics may crash on `import polars` during YOLO training), use the
`old-cpu` optional extra and force the LTS polars build (same as `detection_tags`):

```bash
uv sync --extra ray --extra old-cpu
uv pip uninstall polars polars-lts-cpu && uv pip install polars-lts-cpu==1.33.1
```

If ultralytics image verification fails with `No module named 'pi_heif'`, `pi-heif` is included in
the `old-cpu` extra — re-run `uv sync --extra ray --extra old-cpu`.

What each piece is for:

- `datapipe-app[clickhouse,ml]` — Ops API/UI (`datapipe-ui`), run history, pipeline graph, ClickHouse run logs.
- `datapipe-ui-ml` — ML Ops UI plugin marker (metrics/training/images pages compiled into `datapipe-ui`).
- `datapipe-app-ml-ops` — ML ops specs (`datapipe_app_ml_ops.ops.ops_specs`), metrics/training panels,
  image-record views; observability plugin entry point (not `datapipe-ml[observability]`).
- `datapipe-label-studio` — Label Studio pipeline steps.
- `datapipe-ml[torch,fiftyone,tensorflow]` — YOLO + TF classification training/inference/metrics and
  FiftyOne table stores; pulls in `datapipe-core[s3fs]` for listing and downloading images from
  S3/MinIO in `steps.py`.

Build Ops UI static assets once from the monorepo root (needed for `datapipe api` to serve `/`):

```bash
cd ../.. && yarn install && yarn workspace @datapipe/ui build:package
```

### Local services

Start Postgres, MinIO, MongoDB, FiftyOne App, and Label Studio from this directory:

```bash
docker compose up
```

Wait until Label Studio is ready.

Services:

- Postgres — `localhost:5432` (`postgres` / `password`)
- MinIO — `localhost:9000` (API), `localhost:9001` (console), bucket `datapipe-e2e` (only needed for the sample-data quick start; see [Data ingest](#data-ingest))
- MongoDB — `localhost:27017` (FiftyOne dataset metadata; set `FIFTYONE_DATABASE_URI` in `.env`)
- FiftyOne App — `http://localhost:5151` (reads datasets from MongoDB; mounts `DATAPIPE_E2E_TMP_DIR` for local image paths)
- Label Studio — `http://localhost:8080`

Copy env vars and set the Label Studio API token (after `docker compose up`):

```bash
cp .env.example .env
set -a && source .env && set +a
```

Either paste the token into `.env` manually, or run the helper script:

**Option A — from the Label Studio UI (no script):** open `http://localhost:8080`, sign in, go to **Account & Settings → Access Token**, copy the token, and set `export LABEL_STUDIO_API_KEY=...` in `.env`.

**Option B — via script (optional):**

```bash
sed -i "s/^export LABEL_STUDIO_API_KEY=.*/export LABEL_STUDIO_API_KEY=$(uv run python scripts/label_studio_token.py)/" .env
```

Then load the updated env:

```bash
set -a && source .env && set +a
```

The script reads `LABEL_STUDIO_URL`, `LABEL_STUDIO_EMAIL`, and `LABEL_STUDIO_PASSWORD` from `.env`; use it only if you prefer not to copy the token from the UI.

## Data ingest

Everything is rooted at a single `DATAPIPE_E2E_DIR` (a local path or `s3://` URL). The first stage (`list_s3_images` in `steps.py`) lists images under `$DATAPIPE_E2E_DIR/images/` with extensions `.jpg`, `.jpeg`, `.png`, or `.webp`. The listing is **recursive**, so anything under `images/` is treated as input.

The pipeline `working_dir` (models, resized images, training artifacts) is `$DATAPIPE_E2E_DIR/datapipe/` — a **sibling** of `images/`, never nested under it. This is why both share one root yet input listing never re-ingests its own crops (which would otherwise grow image counts, e.g. 72 instead of 20).

### Sample data (local MinIO)

Default quick start uses MinIO from `docker compose`. With services running and `.env` loaded:

```bash
uv run python scripts/seed_sample_data.py
```

The first run downloads YOLO smoke weights into `sample_data/models/` (`yolo11n.pt`, `yolo11n-pose.pt`),
downloads COCO annotations once into `~/.cache/datapipe/coco/` (~241MB), then fetches sample JPEGs
(cat/dog + person keypoints by default) and uploads them to `s3://datapipe-e2e/images/`.
Pass `--classification-animal-limit` / `--classification-no-animal-limit` to also seed Has Animal /
No Animals images and write `sample_data/classification_labels.json` for the classification
`seed-gt` stage. Re-runs reuse the cache after validating size, zip integrity, and required JSON
entries. Override with `DATAPIPE_CACHE_DIR`. Postgres schemas (`DB_SCHEMA_DETECTION`,
`DB_SCHEMA_KEYPOINTS`, `DB_SCHEMA_CLASSIFICATION`) are created later by `uv run datapipe db create-all`
(see [Running](#running)).

Options:

```bash
uv run python scripts/seed_sample_data.py --detection-limit 10 --keypoints-limit 10
uv run python scripts/seed_sample_data.py --classification-animal-limit 12 --classification-no-animal-limit 12
uv run python scripts/seed_sample_data.py --skip-download   # upload existing sample_data/
```

`.env` uses two MinIO endpoints: `S3_ENDPOINT_URL=http://localhost:9000` for datapipe on the host, and `LABEL_STUDIO_S3_ENDPOINT_URL=http://minio:9000` for Label Studio inside Docker (same compose network as the `minio` service). Task `image_url` values use `S3_PUBLIC_URL` (`http://localhost:9000/...`) so the browser can load images directly; the bucket is configured for anonymous read in `docker-compose.yml`.

### Your own S3 bucket

Use this path when images already live in AWS S3 or another S3-compatible store. MinIO is not required.

1. Set `DATAPIPE_E2E_DIR=s3://<bucket>` in `.env` and upload images under `s3://<bucket>/images/` (flat filenames work best — subdirectory paths are flattened to `___` in `image_name`).
2. Set valid credentials in `.env`:
   - `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`
   - `DATAPIPE_E2E_DIR` (e.g. `s3://my-bucket`; the Label Studio S3 storage bucket is derived from it)
   - For AWS S3, omit `S3_ENDPOINT_URL` and `LABEL_STUDIO_S3_ENDPOINT_URL` so datapipe and Label Studio use the default AWS endpoint.
   - Set `S3_PUBLIC_URL` to a base URL the Label Studio browser can fetch. URLs are built as `$S3_PUBLIC_URL/<bucket>/images/<key>` (path-style), e.g. `https://s3.us-east-1.amazonaws.com` for a publicly readable bucket, or a CDN/proxy in front of your objects.
3. Remove MinIO from `docker-compose.yml`: delete the `minio` and `minio-init` services and drop their `depends_on` entries from `label-studio`. Keep `postgres`, `mongo`, `fiftyone`, and `label-studio`.
4. Skip `scripts/seed_sample_data.py`. Create Postgres schemas before the first pipeline run:

```bash
cd image_detection && uv run datapipe db create-all
cd ../image_keypoints && uv run datapipe db create-all
cd ../image_classification && uv run datapipe db create-all
```

For self-hosted S3-compatible storage (not AWS), keep `S3_ENDPOINT_URL` for datapipe on the host and set `LABEL_STUDIO_S3_ENDPOINT_URL` to an endpoint reachable from the Label Studio container (same pattern as MinIO, but pointing at your store).

## Customization

Most task-specific settings live in each template's `config.py`:

- **Paths** — `DATAPIPE_E2E_DIR` is the single root: input images come from `$DATAPIPE_E2E_DIR/images/` and `working_dir` (models, derived images) is `$DATAPIPE_E2E_DIR/datapipe/`. They are siblings by construction (see [Data ingest](#data-ingest)).
- **Label Studio UI** — `LABEL_CONFIG` (XML labeling interface) and `PROJECT_NAME`. Label names in `LABEL_CONFIG` must match `CLASSES_TO_KEEP` (and `KEYPOINTS_LABELS` for keypoints; `CLASSIFICATION_CLASSES` for classification).
- **Detection** — `CLASSES_TO_KEEP` filters predictions/annotations; `COCO_CLASSES` and `DETECTION_MODEL_CONFIG` set the YOLO class list and pretrained weights (`yolo11n.pt` by default).
- **Keypoints** — `KEYPOINTS_LABELS` defines keypoint order for LS ↔ datapipe conversion; `CLASSES_TO_KEEP` filters bbox class; `KEYPOINTS_MODEL_CONFIG` and `COCO_PERSON_KEYPOINT_FLIP_IDX` configure the pose model and flip augmentation.
- **Classification** — `CLASSIFICATION_CLASSES` = `Has Animal` / `No Animals` (Choices in Label Studio). Seed labels come from COCO animal vs non-animal images via `sample_data/classification_labels.json`.

Training hyperparameters (`epochs`, `batch`, `imgsz`, base checkpoint) are in `app.py` inside
`YoloV8_TrainingConfig` / `TF_ClassificationTrainingConfig`. If you rename LS control tags in
`LABEL_CONFIG`, update the matching logic in `steps.py` (`bboxes_to_ls_prediction`,
`parse_annotations_from_label_studio`, or the Choices parser for classification).

## Running

Run by stage with `uv run datapipe --executor RayExecutor step --labels=... run`. Plain
`uv run datapipe --executor RayExecutor run` executes the whole pipeline without label filtering.

Detection:

```bash
cd image_detection
set -a && source ../.env && set +a
uv run datapipe db create-all
uv run datapipe --executor RayExecutor step --labels=stage=annotation run
```

After tasks appear in Label Studio, annotate them and sync back:

```
uv run datapipe --executor RayExecutor step --labels=stage=annotation run
```

Then train model and visualize results in fiftyone:
```bash
uv run datapipe --executor RayExecutor step --labels=stage=train run
uv run datapipe --executor RayExecutor step --labels=stage=fiftyone run
```

After the FiftyOne stage, open the dataset in the FiftyOne App (`docker compose` must be running):

### FiftyOne App

- Detection — `http://localhost:5151` → dataset `datapipe_detection_e2e`
- Keypoints — `http://localhost:5151` → dataset `datapipe_keypoints_e2e`
- Classification — `http://localhost:5151` → dataset `datapipe_classification_e2e`

The compose `fiftyone` service mounts `DATAPIPE_E2E_TMP_DIR` (default `/tmp/datapipe-e2e`) read-only so sample filepaths from the host pipeline resolve inside the container.

**Local launch (without the Docker service):** if you prefer the FiftyOne process on the host (same machine as the pipeline), with `.env` loaded and `mongo` running:

```bash
set -a && source ../.env && set +a   # from image_detection/ or image_keypoints/
uv run fiftyone app launch datapipe_detection_e2e   # or datapipe_keypoints_e2e
```

This opens the App in your browser and attaches directly to the named dataset. Use it when compose `fiftyone` is stopped or when image paths are not under `DATAPIPE_E2E_TMP_DIR` (e.g. a local `DATAPIPE_E2E_DIR` on disk — then the host App sees files natively; add a matching bind mount to the `fiftyone` service if you still want the Docker App).

Keypoints:

```bash
cd image_keypoints
set -a && source ../.env && set +a
uv run datapipe db create-all
uv run datapipe --executor RayExecutor step --labels=stage=annotation run
```

After tasks appear in Label Studio, annotate them and sync back:
```
uv run datapipe --executor RayExecutor step --labels=stage=annotation run
```

Then train model and visualize results in fiftyone:

```bash
uv run datapipe --executor RayExecutor step --labels=stage=ls-sync run
uv run datapipe --executor RayExecutor step --labels=stage=train run
uv run datapipe --executor RayExecutor step --labels=stage=fiftyone run
```

Then open `http://localhost:5151` and select `datapipe_keypoints_e2e` (or use local launch below).

Classification:

```bash
cd image_classification
set -a && source ../.env && set +a
uv run datapipe db create-all
```

Unattended demo (skip Label Studio; inject COCO seed labels):

```bash
uv run datapipe --executor RayExecutor step --labels=stage=seed-gt run
uv run datapipe --executor RayExecutor step --labels=stage=train run
uv run datapipe --executor RayExecutor step --labels=stage=fiftyone run
```

Or annotate in Label Studio:

```bash
uv run datapipe --executor RayExecutor step --labels=stage=annotation run
# → annotate ≥10 images in LS (:8080), mark completed →
uv run datapipe --executor RayExecutor step --labels=stage=annotation run
uv run datapipe --executor RayExecutor step --labels=stage=train run
uv run datapipe --executor RayExecutor step --labels=stage=fiftyone run
```

TF training needs a GPU, or set `CUDA_VISIBLE_DEVICES=` (empty) to allow CPU. Open
`http://localhost:5151` → dataset `datapipe_classification_e2e`.

## Running via Ops UI

Each pipeline needs its own agent process (distinct port). `pipeline_id` is inferred from the pipeline
module filename (e.g. `app.py` → `app`); observability DB defaults to the pipeline `DB_URL`.

Ops specs are registered in each template's `app.py` via `app.add_specs([...])` using types from
`datapipe_app_ml_ops.ops.ops_specs`. ML metrics/training enrichments load through the `datapipe-app-ml-ops`
observability plugin (`datapipe.observability` entry point).

Start local services first:

```bash
docker compose up -d
```

Run a pipeline **agent** (with `.env` loaded). Use a distinct port per pipeline:

```bash
cd image_detection
set -a && source ../.env && set +a
uv run datapipe --executor RayExecutor --pipeline app:app api --port 8001
```

Open `http://localhost:8001` (title: `Datapipe Ops · app`). From there you can run stages, view the pipeline graph (Debug), and inspect runs.

For keypoints, port `8002`, and for classification, port `8003`:

```bash
cd image_keypoints
set -a && source ../.env && set +a
uv run datapipe --executor RayExecutor --pipeline app:app api --port 8002
```

```bash
cd image_classification
set -a && source ../.env && set +a
uv run datapipe --executor RayExecutor --pipeline app:app api --port 8003
```

The service-backed test path in `tests/e2e_template/` uses the same stack. See `.github/workflows/e2e-template.yml`.
