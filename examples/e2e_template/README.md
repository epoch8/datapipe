# Datapipe E2E Template

This example contains end-to-end Datapipe pipeline templates:

- `image_detection` for bbox detection.
- `image_keypoints` for bbox + keypoints pose pipelines.

Both templates cover the same lifecycle:

1. Load images from S3-compatible storage.
2. Create/sync tasks and predictions in Label Studio.
3. Parse annotations back into Datapipe tables.
4. Freeze a training dataset, train a model, run inference, and compute metrics.
5. Optionally publish predictions and annotations into FiftyOne.

Each template follows the same layout as a standalone project:

- `config.py` — environment variables and paths.
- `data.py` — `catalog` with input tables.
- `steps.py` — batch functions used by the pipeline.
- `app.py` — `pipeline`, `ds`, and `app` defined at module level.

Pipeline metadata is stored in Postgres. Set `DB_URL` in `.env` before running datapipe commands.
Detection and keypoints use separate Postgres schemas (`DB_SCHEMA_DETECTION`, `DB_SCHEMA_KEYPOINTS`) so their tables do not collide when both pipelines share the same database.

All commands below assume the current directory is `examples/e2e_template/`.

## Installation

Python **3.10–3.12** is required (`datapipe-label-studio` and `datapipe-ml` do not support 3.13+ yet).

### Python packages

Dependencies live in `pyproject.toml`. Install with [uv](https://docs.astral.sh/uv/):

```bash
cd examples/e2e_template

uv sync
```

On CPUs **without AVX2+** (ultralytics may crash on `import polars` during YOLO training), use the LTS polars build instead of the default one pulled in by ultralytics:

```bash
uv sync --extra old-cpu
uv pip install --force-reinstall polars-lts-cpu==1.33.1
```

What each piece is for:

- `datapipe-label-studio` — Label Studio pipeline steps.
- `datapipe-ml[torch,fiftyone]` — YOLO training/inference/metrics and FiftyOne table stores; pulls in
`datapipe-core[s3fs]` for listing and downloading images from S3/MinIO in `steps.py`.

### Local services

Start Postgres, MinIO, MongoDB (FiftyOne), and Label Studio from this directory:

```bash
docker compose up
```

Wait until Label Studio is ready.

Services:

- Postgres — `localhost:5432` (`postgres` / `password`)
- MinIO — `localhost:9000` (API), `localhost:9001` (console), bucket `datapipe-e2e` (only needed for the sample-data quick start; see [Data ingest](#data-ingest))
- MongoDB — `localhost:27017` (FiftyOne dataset metadata; set `FIFTYONE_DATABASE_URI` in `.env`)
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

The first run downloads YOLO smoke weights into `sample_data/models/` (`yolo11n.pt`, `yolo11n-pose.pt`), downloads COCO annotations once into `~/.cache/datapipe/coco/` (~241MB), then fetches ~20 JPEGs and uploads them to `s3://datapipe-e2e/images/`. Re-runs reuse the cache after validating size, zip integrity, and required JSON entries. Override with `DATAPIPE_CACHE_DIR`. Postgres schemas (`DB_SCHEMA_DETECTION`, `DB_SCHEMA_KEYPOINTS`) are created later by `datapipe db create-all` (see [Running](#running)).

Options:

```bash
uv run python scripts/seed_sample_data.py --detection-limit 10 --keypoints-limit 10
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
3. Remove MinIO from `docker-compose.yml`: delete the `minio` and `minio-init` services and drop their `depends_on` entries from `label-studio`. Keep `postgres`, `mongo`, and `label-studio`.
4. Skip `scripts/seed_sample_data.py`. Create Postgres schemas before the first pipeline run:

```bash
cd image_detection && datapipe db create-all
cd ../image_keypoints && datapipe db create-all
```

For self-hosted S3-compatible storage (not AWS), keep `S3_ENDPOINT_URL` for datapipe on the host and set `LABEL_STUDIO_S3_ENDPOINT_URL` to an endpoint reachable from the Label Studio container (same pattern as MinIO, but pointing at your store).

## Customization

Most task-specific settings live in each template's `config.py`:

- **Paths** — `DATAPIPE_E2E_DIR` is the single root: input images come from `$DATAPIPE_E2E_DIR/images/` and `working_dir` (models, derived images) is `$DATAPIPE_E2E_DIR/datapipe/`. They are siblings by construction (see [Data ingest](#data-ingest)).
- **Label Studio UI** — `LABEL_CONFIG` (XML labeling interface) and `PROJECT_NAME`. Label names in `LABEL_CONFIG` must match `CLASSES_TO_KEEP` (and `KEYPOINTS_LABELS` for keypoints).
- **Detection** — `CLASSES_TO_KEEP` filters predictions/annotations; `COCO_CLASSES` and `DETECTION_MODEL_CONFIG` set the YOLO class list and pretrained weights (`yolo11n.pt` by default).
- **Keypoints** — `KEYPOINTS_LABELS` defines keypoint order for LS ↔ datapipe conversion; `CLASSES_TO_KEEP` filters bbox class; `KEYPOINTS_MODEL_CONFIG` and `COCO_PERSON_KEYPOINT_FLIP_IDX` configure the pose model and flip augmentation.

Training hyperparameters (`epochs`, `batch`, `imgsz`, base checkpoint) are in `app.py` inside `YoloV8_TrainingConfig`. If you rename LS control tags in `LABEL_CONFIG`, update the matching logic in `steps.py` (`bboxes_to_ls_prediction`, `parse_annotations_from_label_studio`).

## Running

Run by stage with `datapipe step --labels=... run`. Plain `datapipe run` executes the whole pipeline without label filtering.

Detection:

```bash
cd image_detection
datapipe db create-all
datapipe step --labels=stage=annotation run
```

After tasks appear in Label Studio, annotate them and sync back:

```bash
datapipe step --labels=stage=ls-sync run
datapipe step --labels=stage=train run
datapipe step --labels=stage=fiftyone run
```

After the FiftyOne stage, open the dataset in the FiftyOne App (with `.env` loaded and `mongo` running):

```bash
set -a && source ../.env && set +a   # from image_detection/
fiftyone app launch datapipe_detection_e2e
```

Keypoints:

```bash
cd image_keypoints
datapipe db create-all
datapipe step --labels=stage=annotation run
```

After tasks appear in Label Studio, annotate them and sync back:

```bash
datapipe step --labels=stage=ls-sync run
datapipe step --labels=stage=train run
datapipe step --labels=stage=fiftyone run
```

```bash
set -a && source ../.env && set +a   # from image_keypoints/
fiftyone app launch datapipe_keypoints_e2e
```

## Running via Ops UI

Set Ops env vars in `.env` (see `.env.example`). Each pipeline needs its own agent process and a unique `DATAPIPE_APP_PIPELINE_ID` (`image_detection_e2e` or `image_keypoints_e2e`).

Start local services first:

```bash
docker compose up -d
```

Run a pipeline **agent** (with `.env` loaded). Use a distinct port per pipeline:

```bash
cd image_detection
set -a && source ../.env && set +a
datapipe --pipeline app:app api --port 8001
```

Open `http://localhost:8001` (title: `Datapipe Ops · image_detection_e2e`). From there you can run stages, view the pipeline graph (Debug), and inspect runs.

For keypoints, use `image_keypoints_e2e` in `.env` (or override `DATAPIPE_APP_PIPELINE_ID`) and port `8002`.

The service-backed test path in `tests/e2e_template/` uses the same stack. See `.github/workflows/e2e-template.yml`.
