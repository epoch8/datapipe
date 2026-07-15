# detection_tags

A self-contained datapipe detection example built around **tags** (per-scenario metrics), with a
**FiftyOne** view and **no Label Studio**. Ground truth is injected directly (COCO labels, lowercase
`cat`/`dog`), so it runs unattended from scratch.

The point: train a baseline (model A), then add a **tagged scenario TRAIN batch** and retrain
(model B), and watch recall on that tag rise — visible in a dedicated `tag_metrics` table. The demo
is split into **two parts around a checkpoint** so you can prep part 1 and rehearse part 2.

**Claude Code skill:** `/setup-detection-tags` — full deploy, frozen-val order, UI workflow, troubleshooting.

## Installation

Python **3.10–3.12** (`datapipe-ml` does not support 3.13+ yet). From `examples/detection_tags/`:

```bash
uv sync --extra ray    # modern hosts: unmodified — do not edit/re-lock deps (drifts training across machines)
```

Pipeline steps use `RayExecutor` for parallel I/O, inference, and metrics. Install the `ray` extra
before running `datapipe step` or `datapipe api`.

On **pre-AVX2 CPUs** (e.g. epoch8 gpu5), after `uv sync`:

```bash
uv sync --extra ray --extra old-cpu
uv pip uninstall polars polars-lts-cpu && uv pip install polars-lts-cpu==1.33.1
```

Packages (see `pyproject.toml`):

- `datapipe-app[clickhouse,ml]` — Ops API/UI (`datapipe-ui`), run observability, ClickHouse run logs.
- `datapipe-ui-ml` — ML Ops UI plugin marker (metrics/training/images pages compiled into `datapipe-ui`).
- `datapipe-app-ml-ops` — `cat_dog` ops spec, metrics/training panels (`datapipe_app_ml_ops.ops_specs`).
- `datapipe-ml[torch,fiftyone]` — YOLO train/infer/metrics and FiftyOne table stores.

Build Ops UI static assets once from the monorepo root (needed for `datapipe api` to serve `/`):

```bash
cd ../.. && yarn install && yarn workspace @datapipe/ui build:package
```

## Deploy from scratch
```bash
cp .env.example .env && set -a && source .env && set +a
HOST_UID=$(id -u) HOST_GID=$(id -g) docker compose up -d   # postgres + minio + mongo + fiftyone (:5151) + clickhouse (:8123)
cd detection
# DB_SCHEMA defaults to `public`. For a dedicated schema: psql "$DB_URL" -c "CREATE SCHEMA IF NOT EXISTS $DB_SCHEMA"
datapipe db create-all
```

## Ops UI

From `examples/detection_tags/detection` (with `.env` sourced):

```bash
set -a && source ../.env && set +a
uv run datapipe --executor RayExecutor --pipeline app api --host 127.0.0.1 --port 8000
```

Open `http://localhost:8000` (SSH tunnel `-L 8000:localhost:8000` on remote hosts). The front shows
the pipeline graph, training status/curves, and the **`cat_dog`** ops spec (`model_metrics` +
`tag_metrics` tables). Specs are defined in `detection/app.py` via `datapipe_app_ml_ops.ops.ops_specs`.

## Frozen val (why the load order matters)
A `val` metric is an aggregate over whatever images are in val; if you add the tagged batch to val at
retrain time, model A's numbers move and the A-vs-B comparison is invalid. So **freeze val up front**:
load `base-val` + `night-val` (pinned `--subset val`) before training A, and add `night-train`
(`--subset train`) only for part 2. `add_request.py --subset` pins a batch (`image__subset_hint`),
the split step honors it. The pre-staged cache holds **500 images**, so keep the total ≤ 500.

## Identical metrics on any machine — demo seed (no training)

Live-training numbers are bit-stable on one GPU but DIFFER across GPU models (proven — different CUDA
kernels; no software setting fixes it). For the same numbers everywhere, restore the reference state
instead of training (~2 min): models A/B, all metrics and run history land exactly as validated.

```bash
# stack up + .env sourced + venv built + image cache present (scripts/build_cache.py)
bash scripts/restore_demo_seed.sh        # wipes the public schema, restores demo_seed/db.sql.gz,
                                         # re-uploads images byte-identically from the cache
```

Then open the front — `cat_dog` shows the reference metrics (A val recall 0.324 / B 0.415 etc.) in the
Ops UI (`uv run datapipe --executor RayExecutor --pipeline app api`, port 8000). Live (re)training stays available; treat its numbers
as per-GPU.

## Part 1 — baseline to checkpoint
```bash
# from examples/detection_tags/detection
set -a && source ../.env && set +a
python ../scripts/add_request.py --id base-train --n 200 --offset 0   --subset train
python ../scripts/add_request.py --id base-val   --n 75  --offset 200 --subset val
python ../scripts/add_request.py --id night-val  --n 75  --offset 275 --subset val --tag night --darken 0.40
uv run datapipe --executor RayExecutor step --labels=stage=load run
uv run datapipe --executor RayExecutor step --labels=stage=train run            # model A
uv run datapipe --executor RayExecutor step --labels=stage=count-metrics run    # re-run once if it prints "Batches to process 0"
# (demo-only) snapshot the post-A state to rehearse part 2 later, before the fiftyone stage:
docker exec <pg> pg_dump -U postgres -n "$DB_SCHEMA" postgres > /tmp/checkpoint.sql
uv run datapipe --executor RayExecutor step --labels=stage=fiftyone run         # GT + model-A predictions into FiftyOne
```

## Part 2 — retrain and watch the tag metric rise
```bash
python ../scripts/add_request.py --id night-train-a --n 50 --offset 350 --subset train --tag night --darken 0.30
python ../scripts/add_request.py --id night-train-b --n 50 --offset 400 --subset train --tag night --darken 0.40
python ../scripts/add_request.py --id night-train-c --n 50 --offset 450 --subset train --tag night --darken 0.55
uv run datapipe --executor RayExecutor step --labels=stage=load run
uv run datapipe --executor RayExecutor step --labels=stage=train run            # model B (night now in training)
uv run datapipe --executor RayExecutor step --labels=stage=count-metrics run    # re-run once if "0 batches"
uv run datapipe --executor RayExecutor step --labels=stage=fiftyone run         # adds predictions_model_b
```
Rehearse (demo-only): restore the snapshot + wipe the FiftyOne db, then re-run part 2 — no retraining
of model A:
```bash
docker exec <pg> psql -U postgres -c "DROP SCHEMA IF EXISTS $DB_SCHEMA CASCADE; CREATE SCHEMA $DB_SCHEMA"
docker exec -i <pg> psql -U postgres < /tmp/checkpoint.sql
docker exec <mongo> mongosh --quiet --eval "db.getSiblingDB('fiftyone').dropDatabase()"
```

## What you get
- `pipeline_model__metrics_on_subset` — overall metrics per (model, subset).
- **`pipeline_model__metrics_by_tag_on_subset`** — per `(detection_model_id, tag_id, subset_id)`.
  Compare model A vs B at `tag_id=night, subset_id=val`: weighted recall/F1 rise after retraining.
- Class tables: `pipeline_model__metrics_by_cls_on_subset`, `pipeline_model__metrics_by_tag_by_cls_on_subset`.
- **FiftyOne** dataset `$FIFTYONE_DATASET_NAME`: fields `annotations`, `predictions_model_a`,
  `predictions_model_b`; sample fields `tag_id` / `subset_id`. After `stage=fiftyone`, open the App
  from docker compose: **http://localhost:5151** (remote → SSH tunnel `-L 5151:localhost:5151`).
  Compose mounts `DATAPIPE_TAGS_TMP_DIR` (default `/tmp/datapipe-tags`); local images land in
  `$DATAPIPE_TAGS_TMP_DIR/local_images`.

### Pre-staged cache (fast loads)
If `DATAPIPE_TAGS_CACHE_DIR/gt.json` + `DATAPIPE_TAGS_CACHE_DIR/images/<file>.jpg` exist, the load
step reads from them instead of fetching COCO. Default dir `/tmp/datapipe-tags-cache`. It caps the
image pool at its size (500 here) — expand it or point at a fresh dir to download more from COCO.

## Notes
- Classes are lowercase (`cat`/`dog`) to match COCO so injected GT and predictions align.
- Trust `detection_training_status.status`, not exit codes; `count-metrics` may need a second run.
- FiftyOne integration is ported from `examples/e2e_template/image_detection`.
