# How to Run the Detection Tags Pipeline

Full walkthrough for `examples/detection_tags/` — a self-contained cat/dog detection pipeline with **per-tag metrics**, **FiftyOne** visualization, and **no Label Studio**. Ground truth is injected from COCO; you train a baseline (model A), add a tagged night-train batch, retrain (model B), and watch recall on the `night` tag rise.

## Goal

Run the two-part demo: freeze val up front, train model A, checkpoint, add night training data, retrain model B, compare tag metrics in Ops UI and FiftyOne.

## Prerequisites

- Python **3.10–3.12**
- [uv](https://docs.astral.sh/uv/)
- Docker (Postgres, MinIO, MongoDB, FiftyOne, ClickHouse)
- Optional: NVIDIA GPU
- ~2 GB disk for the pre-staged image cache (1000 COCO cat/dog images)

Build Ops UI assets once from the monorepo root:

```bash
cd ../.. && yarn install && yarn workspace @datapipe/ui build:package
```

## Example repo

| Path | Role |
|------|------|
| `examples/detection_tags/` | uv project, `docker-compose.yml`, `.env.example` |
| `examples/detection_tags/detection/` | Pipeline module (`app.py`, `config.py`, `data.py`, `steps.py`) |
| `examples/detection_tags/scripts/add_request.py` | Insert load batches into `load_request` |
| `examples/detection_tags/scripts/build_cache.py` | Build COCO cache locally (alternative to public bucket) |

Pipeline stages use these label values:

| Label | Steps |
|-------|-------|
| `stage=load` | Download COCO images, upload to object storage, write GT + tags |
| `stage=train`, `stage=train-prepare` | Split, freeze dataset, train YOLOv8n, inference |
| `stage=count-metrics`, `stage=tag-metrics` | Subset and per-tag metrics |
| `stage=fiftyone` | Publish GT and model A/B predictions to FiftyOne |

See [Filter Steps by Labels](./filter-by-labels.md).

---

## Step 1 — Install dependencies

From `examples/detection_tags/`:

```bash
cd examples/detection_tags
uv sync --extra ray
```

On **pre-AVX2 CPUs**:

```bash
uv sync --extra ray --extra old-cpu
uv pip uninstall polars polars-lts-cpu && uv pip install polars-lts-cpu==1.33.1
```

---

## Step 2 — Start Docker services

```bash
cp .env.example .env && set -a && source .env && set +a
HOST_UID=$(id -u) HOST_GID=$(id -g) docker compose up -d
```

Services: Postgres, MinIO, MongoDB, FiftyOne App (`http://localhost:5151`), ClickHouse (`:8123`).

Create the database schema:

```bash
cd detection
set -a && source ../.env && set +a
uv run datapipe db create-all
```

---

## Step 3 — Fetch the pre-staged image cache

The load step reads from cache when present (required where COCO downloads are blocked):

```bash
set -a && source ../.env && set +a
BASE=https://storage.yandexcloud.net/e8-demo/datasets/coco-cat-dog-1000
mkdir -p "$DATAPIPE_TAGS_CACHE_DIR/images" && cd "$DATAPIPE_TAGS_CACHE_DIR"
curl -sf "$BASE/gt.json" -o gt.json
python -c "import json; print('\n'.join(json.load(open('gt.json'))))" \
  | xargs -P 16 -I{} curl -sf -o "images/{}" "$BASE/images/{}"
cd ../../detection
```

Alternative on a COCO-reachable host:

```bash
python ../scripts/build_cache.py 1000
```

The cache caps the image pool at 1000 — keep total batch size ≤ 1000.

---

## Step 4 — Part 1: baseline to checkpoint

All commands from `examples/detection_tags/detection` with `.env` loaded.

### Queue load requests (freeze val up front)

```bash
set -a && source ../.env && set +a
python ../scripts/add_request.py --id base-train --n 400 --offset 0   --subset train
python ../scripts/add_request.py --id base-val   --n 150 --offset 400 --subset val
python ../scripts/add_request.py --id night-val  --n 150 --offset 550 --subset val --tag night --darken 0.40
```

`--subset train|val` pins every image in the batch to that split so val stays frozen when you add training data later.

### Run load → train → metrics → FiftyOne

```bash
uv run datapipe --executor RayExecutor step --labels=stage=load run
uv run datapipe --executor RayExecutor step --labels=stage=train run
uv run datapipe --executor RayExecutor step --labels=stage=count-metrics run
```

Re-run `count-metrics` once if it prints **"Batches to process 0"** — upstream may still be settling.

Optional demo checkpoint (before FiftyOne, to rehearse part 2):

```bash
docker exec <pg> pg_dump -U postgres -n "$DB_SCHEMA" postgres > /tmp/checkpoint.sql
uv run datapipe --executor RayExecutor step --labels=stage=fiftyone run
```

---

## Step 5 — Part 2: retrain and watch tag metrics rise

Add the tagged **train** batch (not val — val was frozen in part 1):

```bash
python ../scripts/add_request.py --id night-train-a --n 100 --offset 700 --subset train --tag night --darken 0.30
python ../scripts/add_request.py --id night-train-b --n 100 --offset 800 --subset train --tag night --darken 0.40
python ../scripts/add_request.py --id night-train-c --n 100 --offset 900 --subset train --tag night --darken 0.55
uv run datapipe --executor RayExecutor step --labels=stage=load run
uv run datapipe --executor RayExecutor step --labels=stage=train run
uv run datapipe --executor RayExecutor step --labels=stage=count-metrics run
uv run datapipe --executor RayExecutor step --labels=stage=fiftyone run
```

Compare model A vs model B at `tag_id=night, subset_id=val` in the tag metrics table — weighted recall/F1 should rise after retraining.

### Rehearse part 2 (demo-only)

Restore checkpoint without retraining model A:

```bash
docker exec <pg> psql -U postgres -c "DROP SCHEMA IF EXISTS $DB_SCHEMA CASCADE; CREATE SCHEMA $DB_SCHEMA"
docker exec -i <pg> psql -U postgres < /tmp/checkpoint.sql
docker exec <mongo> mongosh --quiet --eval "db.getSiblingDB('fiftyone').dropDatabase()"
```

---

## Step 6 — Ops UI

From `examples/detection_tags/detection`:

```bash
set -a && source ../.env && set +a
uv run datapipe --executor RayExecutor --pipeline app api --host 127.0.0.1 --port 8000
```

Open **http://localhost:8000** (remote host: SSH tunnel `-L 8000:localhost:8000`).

The **`cat_dog`** ops spec exposes:

| Table / view | Content |
|--------------|---------|
| `model_metrics` | Overall metrics per (model, subset) |
| `tag_metrics_on_subset` | Per `(detection_model_id, tag_id, subset_id)` |
| `subset_class_metrics` / `tag_class_metrics_on_subset` | Per-class breakdown |
| Training panel | YOLO runs, frozen datasets, training requests |
| Image records | GT, predictions, subset and tag filters |

See [DatapipeApp and API](../ops/datapipe-app.md).

---

## Step 7 — FiftyOne

After `stage=fiftyone`, open **http://localhost:5151** (tunnel `-L 5151:localhost:5151` on remote hosts).

Dataset: `$FIFTYONE_DATASET_NAME` from `.env`.

| Field | Content |
|-------|---------|
| `annotations` | Ground truth |
| `predictions_model_a` | Baseline model |
| `predictions_model_b` | Retrained model |
| Sample fields `tag_id`, `subset_id` | Filter by tag and split |

Local images: `$DATAPIPE_TAGS_TMP_DIR/local_images` (default `/tmp/datapipe-tags/local_images`).

---

## Verify

```bash
# Rows loaded
uv run datapipe table s3_images list | wc -l

# Two trained models after part 2
uv run datapipe table detection_model_train list

# Tag metrics on val — compare models at tag_id=night
uv run datapipe table pipeline_model__metrics_by_tag_on_subset list
```

In Ops UI, filter `tag_metrics_on_subset` with `subset_id=val` and `tag_id=night`. Model B should show higher weighted recall than model A.

---

## Expected result

- `pipeline_model__metrics_on_subset` — overall metrics per (model, subset).
- **`pipeline_model__metrics_by_tag_on_subset`** — per-tag metrics; the demo's key table.
- FiftyOne dataset with GT, model A predictions, and model B predictions.
- Classes are lowercase `cat`/`dog` to match COCO injection.

## Notes

- Trust `detection_training_status.status`, not CLI exit codes.
- `count-metrics` may need a second run if the first prints 0 batches.
- `--darken` simulates low-light images for the `night` tag (gamma &lt; 1).
- No Label Studio — GT comes from the load step. For the LS-based flow, see [E2E Image Detection Walkthrough](../getting-started/e2e-image-detection-walkthrough.md).

## See also

- Example README: [`examples/detection_tags/README.md`](https://github.com/epoch8/datapipe/tree/master/examples/detection_tags)
- [datapipe-ml](../integrations/datapipe-ml.md)
- [Run Steps with RayExecutor](./ray-executor.md)
- [Troubleshooting](../troubleshooting.md)
