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

All commands below assume the current directory is **`examples/e2e_template/`**.

## Installation

Python **3.10–3.12** is required (`datapipe-label-studio` and `datapipe-ml` do not support 3.13+ yet).

### Python packages

Install workspace packages with `uv` from this directory:

```bash
cd examples/e2e_template

uv pip install \
  -e "../../libs/datapipe-label-studio" \
  -e "../../libs/datapipe-ml[torch,fiftyone]"
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
- MinIO — `localhost:9000` (API), `localhost:9001` (console), bucket `datapipe-e2e`
- MongoDB — `localhost:27017` (FiftyOne dataset metadata; set `FIFTYONE_DATABASE_URI` in `.env`)
- Label Studio — `http://localhost:8080`

`.env` uses two MinIO endpoints: `S3_ENDPOINT_URL=http://localhost:9000` for datapipe on the host, and `LABEL_STUDIO_S3_ENDPOINT_URL=http://minio:9000` for Label Studio inside Docker (same compose network as the `minio` service). Task `image_url` values use `S3_PUBLIC_URL` (`http://localhost:9000/...`) so the browser can load images directly; the bucket is configured for anonymous read in `docker-compose.yml`.

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

Seed sample images (10 cat/dog + 10 person keypoint photos from COCO train2017, same source as ML smoke tests):

```bash
uv run python scripts/seed_sample_data.py
```

The first run creates both Postgres schemas (`DB_SCHEMA_DETECTION`, `DB_SCHEMA_KEYPOINTS`), downloads COCO annotations once into `~/.cache/datapipe/coco/` (~241MB), then fetches ~20 JPEGs and uploads them to `s3://datapipe-e2e/images/`. Re-runs reuse the cache after validating size, zip integrity, and required JSON entries. Override with `DATAPIPE_CACHE_DIR`.

Options:

```bash
uv run python scripts/seed_sample_data.py --detection-limit 10 --keypoints-limit 10
uv run python scripts/seed_sample_data.py --skip-download   # upload existing sample_data/
```

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

Install `datapipe-app` (in addition to the packages above):

```bash
uv pip install -e "../../libs/datapipe-app"
```

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
