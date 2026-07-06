---
name: setup-detection-tags
description: >
  Use when setting up or running examples/detection_tags — a self-contained datapipe detection
  demo built around tags (per-scenario metrics), with no Label Studio and no FiftyOne, deployed
  from scratch; or when demoing "add a tagged batch, retrain, watch the tag metric rise".
---

# detection_tags (tags demo — no Label Studio, no FiftyOne)

Minimal detection pipeline whose whole point is **tags**: train a baseline, load a **tagged scenario
batch**, retrain, and watch the metric on that tag rise in a `tag_metrics` table. Ground truth is
**injected** (COCO labels, lowercase `cat`/`dog`) — no human annotation — so it runs unattended.

**First, ask: demo (test) or real data?** — the whole flow branches on this.

- **Demo / test** → run it unattended on the built-in COCO cat/dog data. Do it in this order and
  *narrate each step*:
  1. deploy (services + `uv sync` + `db create-all`), load the **base** batch, run `stage=train`;
  2. **show the metrics** (`detection_model_train__metrics_on_subset`) for the baseline model;
  3. say: *"there's a `night` low-light tagged scenario — want to add it and retrain, or stop here?"*
  4. if yes → load the **night** batch (`--tag night --darken 0.25`), run `stage=train` again;
  5. after the run, **show the metrics again** — both `metrics_on_subset` and **`tag_metrics`** (the
     `night/val` recall for the baseline vs the retrained model).

- **Real data** → don't guess; gather everything up front and fill it in explicitly:
  - **images + ground truth**: where are the images, and where do the boxes/labels come from —
    a labelled dataset to inject, or real annotation? (this example has no Label Studio; GT is injected)
  - **classes**: the real class names → set `DETECTION_CLASSES` / the GT labels to match exactly (casing!)
  - **the tag/scenario**: what is the tagged case (e.g. night, occluded, a site) and which images carry it
  - **storage + DB**: which S3/MinIO bucket (`DATAPIPE_TAGS_DIR`) and which Postgres/schema (`DB_URL`/`DB_SCHEMA`)
  - **GPU + training size**: enough data and epochs that metrics are meaningful (see the note below)
  Then wire the loader to the real source instead of the COCO `load_batch` step.

**Ask first — don't assume (only the unresolved):** which Postgres + which database for `DB_URL`
(don't target an existing DB or drop a `localhost` default without confirming); reuse an existing
venv/`uv` env or a fresh one? GPU available (training is GPU)? surface stage logs or run quiet?

**How to work:** propose a short plan and get a go-ahead; show each stage's logs and report what
changed; trust the `*_training_status` table, not the exit code. On an unclear failure re-run with
`datapipe --debug … run` sent to a file + `grep`, not inline.

## Deploy from scratch
```bash
cp .env.example .env && set -a && source .env && set +a   # DB_URL, S3/MinIO, DATAPIPE_TAGS_DIR
docker compose up -d          # postgres + minio ONLY (no mongo, no Label Studio)
uv sync                       # cu124 torch + polars-lts-cpu + pi-heif baked in
cd detection
# db create-all makes the tables, NOT the schema — create it first:
psql "$DB_URL" -c "CREATE SCHEMA IF NOT EXISTS $DB_SCHEMA"   # or: docker exec <pg> psql -U postgres -c "CREATE SCHEMA IF NOT EXISTS datapipe_tags"
datapipe db create-all
```

## Two-step data load — via datapipe steps (no annotation)
Loading is a pipeline step (`stage=load`) driven by rows in the `load_request` table. Add a request,
run the load step; it downloads COCO cat/dog, uploads to MinIO, and emits `s3_images` + ground truth
(+ tag). Labels are lowercase `cat`/`dog`; `image_name` = object basename so all joins line up.
```bash
# from examples/detection_tags/detection
python ../scripts/add_request.py --id base --n 450
datapipe step --labels=stage=load run                                   # batch 1: base cat/dog
python ../scripts/add_request.py --id night --n 50 --offset 450 --tag night --darken 0.25
datapipe step --labels=stage=load run                                   # batch 2: tagged scenario
```

## Run
```bash
datapipe run                  # load → split → freeze → train → inference → metrics → tag_metrics
# or by stage: datapipe step --labels=stage=train run
```

## The payoff
`tag_metrics(detection_model_id, tag_id, subset_id)` → precision/recall/f1. Compare the baseline
(trained before batch 2) vs the retrained model at `tag=night, subset=val` — recall on the tag rises
once the tagged batch is in training. `tag`/`image__tag` are external inputs (produced by the `load` step);
`tag_metrics` is a real pipeline step (`steps.compute_tag_metrics`, `transform_keys=["detection_model_id"]`
so the aggregation is correct).

## Two-model demo (baseline vs retrained)
1. Load batch 1 → `datapipe step --labels=stage=train run` → model A (no tag in training).
2. Load batch 2 (`--tag night --darken 0.25`) → `datapipe step --labels=stage=train run` → model B.
3. Read `tag_metrics`: `night/val` ≈ low for A, higher for B.

## Troubleshooting (may already be fixed — verify against current files)
- **`SIGILL` / `Illegal instruction` in the training subprocess** → `polars` built for a CPU newer
  than the host (pre-AVX2). The `polars-lts-cpu` pin is **not enough** on its own: the regular `polars`
  comes in transitively (datapipe-ml/core) and both install the same `polars` module. After `uv sync`
  force lts-cpu to win: `uv pip uninstall polars polars-lts-cpu && uv pip install polars-lts-cpu==1.33.1`.
- **`No labels found` / every image "corrupt: No module named 'pi_heif'`** → ultralytics image
  verification needs `pi_heif` (pinned here); reinstall if missing.
- **`No ground truth` at freeze** → `image__ground_truth.image_name` must match `s3_images.image_name`
  (the load step uses the object basename for both); a mismatched key makes the join empty.
- **Metrics 0 on a trained model** → tiny/noisy val makes "best epoch" latch onto an early
  checkpoint; use enough data (~500 total via the default batches) and the shipped epoch config.
- **Training exits 0 but no model** → datapipe swallows step errors; check `detection_training_status`.
