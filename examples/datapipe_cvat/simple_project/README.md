# CVAT Annotation Pipeline With Datapipe

## Overview

This example demonstrates a small Datapipe pipeline that discovers local images,
uploads them directly to CVAT, and synchronizes annotations back into Datapipe tables.

The pipeline reads COCO cat/dog images from `input/*.jpg`, loads CVAT preannotations
from `input/annotations/`, uploads files to CVAT, and syncs annotations back into
Datapipe tables.

Sample data uses the same COCO train2017 source as `examples/e2e_template`.

All commands below assume the current directory is **`examples/datapipe_cvat/simple_project/`**.

## Prerequisites

- Python 3.10–3.12
- Docker and Docker Compose (for local CVAT)
- Git
- Installed monorepo packages: `datapipe-core` and `datapipe-cvat`
- A CVAT project (cloud storage is **not** required — files are uploaded locally)

## Installation

From the monorepo root (`datapipe/`):

```bash
pip install -e "libs/datapipe-core[sqlite]"
pip install -e "libs/datapipe-cvat" --no-deps
pip install "cvat-sdk==2.65.0" requests pillow tqdm
```

## Run CVAT locally

The repo ships a helper script that clones [cvat-ai/cvat](https://github.com/cvat-ai/cvat) and
starts it with Docker Compose. CVAT version **v2.65.0** matches `cvat-sdk==2.65.0` used in CI.

From the monorepo root:

```bash
CVAT_VERSION=v2.65.0 CVAT_REPO_DIR=/tmp/cvat libs/datapipe-cvat/tests/start-cvat.sh
```

Or from this example directory:

```bash
CVAT_VERSION=v2.65.0 CVAT_REPO_DIR=/tmp/cvat ../../libs/datapipe-cvat/tests/start-cvat.sh
```

CVAT UI: **http://localhost:8080**

The script stops containers on `Ctrl+C`.

### Create admin user (first run)

In another terminal, after CVAT has started:

```bash
docker exec cvat_server bash -lc \
  "DJANGO_SUPERUSER_PASSWORD=admin python3 ~/manage.py createsuperuser --username admin --email admin@example.com --noinput"
```

Default credentials used in this example: `admin` / `admin`.

### Check that CVAT is ready

```bash
curl -fsS http://localhost:8080/api/server/about
```

### Stop CVAT

If you started CVAT with `start-cvat.sh`, press `Ctrl+C` in that terminal.

Or manually:

```bash
docker compose --project-directory /tmp/cvat -f /tmp/cvat/docker-compose.yml down
```

## Sample data

Download 20 COCO cat/dog images and write CVAT preannotations (`cat` / `dog` bboxes):

```bash
python scripts/seed_sample_data.py
```

The first run downloads COCO annotations once into `~/.cache/datapipe/coco/` (~241MB).
The cache is validated (size, zip integrity, required JSON entries) before reuse.
Then 20 JPEGs land in `input/` and XML fragments in `input/annotations/`.

Override cache location:

```bash
export DATAPIPE_CACHE_DIR=/path/to/cache
```

Options:

```bash
python scripts/seed_sample_data.py --limit 20
python scripts/seed_sample_data.py --skip-download
```

### Create CVAT project

If CVAT is running and `cvat-sdk` is installed:

```bash
export CVAT_URL=http://localhost:8080
export CVAT_USERNAME=admin
export CVAT_PASSWORD=admin

export CVAT_PROJECT_ID=$(python scripts/create_cvat_project.py)
```

The script prints the project id to stdout (logs go to stderr).
Project labels: `cat`, `dog`. Reuses an existing project with the same name.

Alternatively, create a project manually in the CVAT UI (**Projects → Create**).
Missing labels are added automatically when preannotations are imported.

## Environment variables

```bash
export CVAT_URL=http://localhost:8080
export CVAT_USERNAME=admin
export CVAT_PASSWORD=admin
export CVAT_PROJECT_ID=1          # from create_cvat_project.py or CVAT UI
export CVAT_ORGANIZATION=           # empty for personal workspace
```

## Usage

If you already ran the pipeline with the old dataset, reset local state first:

```bash
rm -f db.sqlite*
```

Then:

```bash
datapipe db create-all
datapipe step run
```

With 20 images and `files_batch=5`, the pipeline creates **4 CVAT tasks** with
5 images each (single queue `queue1`).

Images are uploaded to CVAT over HTTP from the local filesystem
(`cloud_storage_bucket=None` in `CVATStep`).

## Troubleshooting

| Symptom | What to check |
|---------|----------------|
| CVAT task has no images | Files missing in `input/`, or wrong absolute paths |
| No preannotations in CVAT | Missing `input/annotations/{image_id}.xml` — rerun `seed_sample_data.py` |
| Old 2-image tasks still appear | Delete `db.sqlite*` and old tasks in CVAT UI, rerun pipeline |
| Connection refused to CVAT | Run `start-cvat.sh` and wait for `curl .../api/server/about` |
