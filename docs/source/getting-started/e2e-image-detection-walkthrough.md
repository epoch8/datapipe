# E2E Image Detection Walkthrough

End-to-end tutorial for the cat/dog detection pipeline in `examples/e2e_template/image_detection/`. You load images from S3-compatible storage, annotate in Label Studio, train YOLOv8, compute metrics, and publish to FiftyOne — all incrementally, stage by stage.

## What you'll build

A production-shaped detection pipeline with four project files:

| File | Role |
|------|------|
| `config.py` | Environment variables, Label Studio config, model defaults |
| `data.py` | `catalog` — table schemas and stores |
| `steps.py` | Batch functions (list images, parse LS annotations, FiftyOne publish) |
| `app.py` | `Pipeline`, `DataStore`, `DatapipeAPI`, Ops specs |

Stages are tagged with `labels=[("stage", "...")]` so you run only the slice you need. See [Filter Steps by Labels](../how-to/filter-by-labels.md) and [Pipeline Steps](../concepts/pipeline-steps.md).

---

## Part 1 — Setup

### Prerequisites

- Python **3.10–3.12** (not 3.13+)
- [uv](https://docs.astral.sh/uv/) for dependency management
- Docker (Postgres, MinIO, MongoDB, FiftyOne, Label Studio, ClickHouse)
- Optional: NVIDIA GPU for faster inference and training

### Install Python packages

All commands assume the current directory is `examples/e2e_template/`:

```bash
cd examples/e2e_template
uv sync --extra ray
```

Pipeline steps use `RayExecutor` for parallel I/O, inference, and metrics. Install the `ray` extra before running `datapipe step` or `datapipe api`. See [Run Steps with RayExecutor](../how-to/ray-executor.md).

On CPUs **without AVX2+** (ultralytics may crash on `import polars`):

```bash
uv sync --extra ray --extra old-cpu
uv pip uninstall polars polars-lts-cpu && uv pip install polars-lts-cpu==1.33.1
```

Build Ops UI static assets once from the monorepo root (required for `datapipe api` to serve `/`):

```bash
cd ../.. && yarn install && yarn workspace @datapipe/ui build:package
cd examples/e2e_template
```

### Start local services

```bash
docker compose up -d
```

Wait until Label Studio is ready at `http://localhost:8080`.

| Service | URL / port |
|---------|------------|
| Postgres | `localhost:5432` (`postgres` / `password`) |
| MinIO | API `localhost:9000`, console `localhost:9001`, bucket `datapipe-e2e` |
| MongoDB | `localhost:27017` (FiftyOne metadata) |
| FiftyOne App | `http://localhost:5151` |
| Label Studio | `http://localhost:8080` |

### Configure environment

```bash
cp .env.example .env
set -a && source .env && set +a
```

Set the Label Studio API token — either copy it from **Account & Settings → Access Token** in the UI, or:

```bash
sed -i "s/^export LABEL_STUDIO_API_KEY=.*/export LABEL_STUDIO_API_KEY=$(uv run python scripts/label_studio_token.py)/" .env
set -a && source .env && set +a
```

### Seed sample data

With services running and `.env` loaded:

```bash
uv run python scripts/seed_sample_data.py
```

This downloads YOLO smoke weights, COCO annotations, and sample cat/dog JPEGs into `s3://datapipe-e2e/images/`. Re-runs reuse the cache. Options:

```bash
uv run python scripts/seed_sample_data.py --detection-limit 10
uv run python scripts/seed_sample_data.py --skip-download   # upload existing sample_data/
```

### Create the database schema

```bash
cd image_detection
set -a && source ../.env && set +a
uv run datapipe db create-all
```

Detection uses schema `DB_SCHEMA_DETECTION` (default `datapipe_e2e_detection`) so it does not collide with keypoints or classification pipelines on the same Postgres instance.

---

## Part 2 — Catalog tour

Understanding the four files before you run stages saves debugging time later.

### `config.py` — environment and task settings

- `DATAPIPE_E2E_DIR` — single root for input images (`…/images/`) and pipeline artifacts (`…/datapipe/`). They are **siblings** so the recursive image listing never re-ingests training crops. See [Tables and TableStores](../concepts/tables-and-stores.md).
- `LABEL_CONFIG` / `PROJECT_NAME` — Label Studio labeling interface (Cat/Dog rectangle labels).
- `CLASSES_TO_KEEP` — filters predictions and annotations to Cat/Dog only.
- `DETECTION_MODEL_CONFIG` — pretrained YOLO weights for pre-annotation.
- `DBCONN`, `CLICKHOUSE_RUN_LOGS_URL` — Postgres metadata and ClickHouse run logs.

### `data.py` — catalog

Declares input and output tables backed by `TableStoreDB` and FiftyOne stores:

- `s3_images` — primary key `image_name`, column `image_url`
- `image__ground_truth` — bbox annotations parsed from Label Studio
- `image__subset` — train/val split
- `detection_model`, `detection_prediction`, `best_detection_model`
- `fiftyone_*` — FiftyOne dataset `datapipe_detection_e2e`

Tables are named resources the pipeline steps read and write. See [Pipeline / Catalog](../reference/pipeline-catalog.md).

### `steps.py` — batch functions

Key functions wired into the pipeline:

| Function | Purpose |
|----------|---------|
| `list_s3_images` | `BatchGenerate` — list images under `$DATAPIPE_E2E_DIR/images/` |
| `get_images_without_ground_truth` | Find images needing annotation |
| `filter_bboxes_by_classes` | Keep only Cat/Dog boxes |
| `bboxes_to_ls_prediction` | Format predictions for Label Studio upload |
| `parse_annotations_from_label_studio` | LS annotations → `image__ground_truth` |
| `split_df_train_val` | Random 75/25 train/val split |
| `download_images` / `publish_to_fiftyone_*` | Local copies and FiftyOne export |

### `app.py` — pipeline graph and Ops specs

The `Pipeline([...])` list defines step order and dependencies. Each step has `labels=[("stage", "...")]`:

| Stage label | Steps (summary) |
|-------------|-----------------|
| `annotation` | List images, pre-annotate, upload LS tasks/predictions, parse annotations |
| `train`, `train-prepare` | Split, freeze dataset, train YOLOv8, inference, metrics, find best model |
| `inference`, `count-metrics` | Sub-labels within train stage |
| `fiftyone` | Download images, publish GT and predictions to FiftyOne |

`DatapipeAPI` wraps the pipeline for CLI and HTTP Ops UI. Ops specs (`cat_dog`) register metrics, training, and image views via `datapipe_app_ml_ops`.

---

## Part 3 — Run `stage=annotation`

The annotation stage lists S3 images, runs a pretrained detector for pre-labels, creates Label Studio tasks, and syncs completed annotations back into Datapipe.

```bash
cd image_detection
set -a && source ../.env && set +a
uv run datapipe --executor RayExecutor step --labels=stage=annotation run
```

### What happens

1. **`list_s3_images`** — discovers new/changed images under `images/`.
2. **`Inference_DetectionModel`** — runs smoke YOLO on images without ground truth.
3. **`LabelStudioUploadTasks`** — creates tasks in project `Datapipe detection e2e`.
4. **`LabelStudioUploadPredictions`** — attaches model pre-labels.
5. **`parse_annotations_from_label_studio`** — writes completed annotations to `image__ground_truth`.

Open Label Studio at `http://localhost:8080`, annotate tasks (draw Cat/Dog boxes, mark **Submitted**), then sync back:

```bash
uv run datapipe --executor RayExecutor step --labels=stage=annotation run
```

Only dirty keys reprocess — unchanged images are skipped. See [Incremental Processing](../concepts/incremental-processing.md).

Integration details: [Label Studio](../integrations/label-studio.md).

### Verify annotation stage

```bash
uv run datapipe table s3_images list | head
uv run datapipe table image__ground_truth list | head
```

Expect rows in `image__ground_truth` for every annotated image.

---

## Part 4 — Run `stage=train`

Training freezes a dataset snapshot, trains YOLOv8, runs inference on train/val, and computes detection metrics.

```bash
uv run datapipe --executor RayExecutor step --labels=stage=train run
```

### What happens

1. **`split_df_train_val`** — assigns 75% train / 25% val (seed 42).
2. **`DetectionFreezeDataset`** — materializes a frozen snapshot when enough new labels arrive (`min_delta=10`, `min_within_time=15min`).
3. **`Train_YoloV8_DetectionModel`** — trains YOLOv8s (640px, 30 epochs) with sync/resume configs.
4. **`Inference_DetectionModel`** — runs the trained model on all images in subsets.
5. **`CountMetrics_Subset_PipelineModel`** — TP/FP/FN per subset and class.
6. **`FindBestModel`** — picks best model by weighted F1 on val.

Training status is tracked in `detection_training_status`. Trust `status`, not CLI exit codes — YOLO training runs asynchronously.

See [datapipe-ml](../integrations/datapipe-ml.md) for ML step details.

### Verify training stage

```bash
uv run datapipe table detection_model list
uv run datapipe table pipeline_model__metrics_on_subset list
```

Expect at least one trained model and val metrics with `calc__weighted_f1_score`.

---

## Part 5 — Run `stage=fiftyone`

Publish ground truth and best-model predictions to FiftyOne for visual QA.

```bash
uv run datapipe --executor RayExecutor step --labels=stage=fiftyone run
```

Open the FiftyOne App (Docker service must be running):

- **http://localhost:5151** → dataset `datapipe_detection_e2e`

Fields: `annotations` (GT with `subset_id`), `predictions_from_best_model`.

**Local launch** (without Docker FiftyOne service):

```bash
set -a && source ../.env && set +a
uv run fiftyone app launch datapipe_detection_e2e
```

Local images land in `$DATAPIPE_E2E_TMP_DIR/local_images` (default `/tmp/datapipe-e2e/local_images`). The compose `fiftyone` service mounts this path read-only.

---

## Part 6 — Ops API and UI

Run the pipeline agent with the Ops dashboard on a dedicated port:

```bash
cd image_detection
set -a && source ../.env && set +a
uv run datapipe --executor RayExecutor --pipeline app:app api --port 8001
```

Open **http://localhost:8001** (title: `Datapipe Ops · app`).

From the UI you can:

- Run stages by label (e.g. `stage=annotation`, `stage=train`)
- Inspect the pipeline graph (Debug)
- Browse runs and ClickHouse run logs
- View **`cat_dog`** ops spec: model metrics, class metrics, frozen datasets, training requests, image records

See [DatapipeApp and API](../ops/datapipe-app.md) and [Ops UI walkthrough](../ops/ui-walkthrough.md).

For keypoints and classification templates, use ports `8002` and `8003` respectively.

---

## Part 7 — Incremental exercise

Practice [incremental processing](../concepts/incremental-processing.md) without re-running the full pipeline.

### Exercise A — Add images, re-annotate only new keys

1. Upload more images to `s3://datapipe-e2e/images/` (or re-run seed with a higher limit).
2. Run annotation stage only:

```bash
uv run datapipe --executor RayExecutor step --labels=stage=annotation run
```

3. Annotate new tasks in Label Studio, sync again.
4. Run train stage — only new/changed ground truth triggers a new frozen dataset when `min_delta` is met:

```bash
uv run datapipe --executor RayExecutor step --labels=stage=train run
```

### Exercise B — Re-run metrics only

After inference outputs change, recompute metrics without retraining:

```bash
uv run datapipe --executor RayExecutor step --labels=stage=count-metrics run
```

If the step prints **"Batches to process 0"**, upstream outputs are unchanged — reset metadata or change inputs first.

### Exercise C — Inspect dirty keys before running

```bash
uv run datapipe --executor RayExecutor step --labels=stage=annotation list
```

Lists steps that would run and how many batches are pending.

---

## Part 8 — Customize the template

Common changes live in `config.py` and `app.py`:

| Setting | File | Effect |
|---------|------|--------|
| Label names | `config.py` `LABEL_CONFIG`, `CLASSES_TO_KEEP` | Must stay in sync with `steps.py` parsers |
| Pretrained weights | `DETECTION_MODEL_CONFIG` | Pre-annotation model |
| Training hyperparams | `app.py` `YoloV8_TrainingConfig` | epochs, batch, imgsz, base checkpoint |
| Val split ratio | `steps.split_df_train_val` kwargs | `val_perc=0.25` |
| S3 bucket | `.env` `DATAPIPE_E2E_DIR` | Point at your own bucket (see e2e README) |

For your own S3 bucket (no MinIO), set `DATAPIPE_E2E_DIR=s3://<bucket>`, configure AWS credentials, and skip `seed_sample_data.py`.

---

## Part 9 — Troubleshooting

| Symptom | Likely cause | Fix |
|---------|--------------|-----|
| `DB_URL is required` | `.env` not loaded | `set -a && source ../.env && set +a` from `image_detection/` |
| `CLICKHOUSE_RUN_LOGS_URL is required` | ClickHouse not in compose or env missing | Start `docker compose up -d`, check `.env` |
| Label Studio tasks have broken images | Wrong `S3_PUBLIC_URL` or bucket ACL | Browser must reach `$S3_PUBLIC_URL/<bucket>/images/...` |
| No tasks in Label Studio | Annotation stage not run or empty `s3_images` | Run seed script, then `stage=annotation` |
| `image__ground_truth` empty after sync | Tasks not marked completed in LS | Submit annotations, re-run annotation stage |
| Training exits immediately, no model | Not enough annotated images for freeze | Annotate ≥10 images; check `detection_training_status` |
| `count-metrics` shows 0 batches | Metrics already up to date | Expected if inputs unchanged; re-run after inference changes |
| FiftyOne shows missing images | Path not mounted in Docker | Use `$DATAPIPE_E2E_TMP_DIR` or launch FiftyOne on host |
| `import polars` crash on old CPU | Non-AVX2 CPU | `uv sync --extra old-cpu` + polars-lts-cpu (see setup) |
| Ops UI blank at `/` | UI assets not built | `yarn workspace @datapipe/ui build:package` from monorepo root |
| GPU not used | `DATAPIPE_USE_GPU=0` or no CUDA | Set `DATAPIPE_USE_GPU=1`, verify `torch.cuda.is_available()` |

General troubleshooting: [Troubleshooting](../troubleshooting.md).

---

## See also

- Example README: [`examples/e2e_template/README.md`](https://github.com/epoch8/datapipe/tree/master/examples/e2e_template)
- [What is Datapipe?](../concepts/what-is-datapipe.md) — core mental model
- [Primary Keys and Transform Keys](../concepts/primary-keys.md)
- [Run Model Inference](../how-to/model-inference.md) — multi-input `Inference_DetectionModel`
- [Run Detection Tags Pipeline](../how-to/run-detection-tags-pipeline.md) — same ML flow without Label Studio
