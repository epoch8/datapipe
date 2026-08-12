# detection_tags

A self-contained datapipe detection example built around **tags** (per-scenario metrics), with a
**FiftyOne** view and **no Label Studio**. Ground truth is injected directly (COCO labels, lowercase
`cat`/`dog`), so it runs unattended from scratch.

The point: train a baseline (model A), then add a **tagged scenario TRAIN batch** and retrain
(model B), and watch recall on that tag rise — visible in per-tag metrics tables. The demo
is split into **two parts around a checkpoint** so you can prep part 1 and rehearse part 2.

## Deploy from scratch
```bash
cp .env.example .env && set -a && source .env && set +a
docker compose up -d            # postgres + minio + mongo (mongo backs FiftyOne)
uv sync                         # cu124 torch + datapipe-ml[torch,fiftyone] + fiftyone + pi-heif
# pre-AVX2 host only: force the lts polars to win (else training SIGILLs)
uv pip uninstall polars polars-lts-cpu && uv pip install polars-lts-cpu==1.33.1
cd detection
# DB_SCHEMA defaults to `public` (exists already). For a dedicated schema (sharing the Postgres with
# other pipelines) create it first: psql "$DB_URL" -c "CREATE SCHEMA IF NOT EXISTS $DB_SCHEMA"
datapipe db create-all
```

## Frozen val (why the load order matters)
A `val` metric is an aggregate over whatever images are in val; if you add the tagged batch to val at
retrain time, model A's numbers move and the A-vs-B comparison is invalid. So **freeze val up front**:
load `base-val` + `night-val` (pinned `--subset val`) before training A, and add night train batches
(`--subset train`) only for part 2. `add_request.py --subset` pins a batch (`image__subset_hint`),
the split step honors it. Keep the total batch size ≤ the pre-staged cache size (see below).

## Part 1 — baseline to checkpoint
```bash
# from examples/detection_tags/detection
python ../scripts/add_request.py --id base-train --n 200 --offset 0   --subset train
python ../scripts/add_request.py --id base-val   --n 75  --offset 200 --subset val
python ../scripts/add_request.py --id night-val  --n 75  --offset 275 --subset val --tag night --darken 0.40
datapipe step --labels=stage=load run
datapipe step --labels=stage=train run            # model A
datapipe step --labels=stage=count-metrics run    # re-run once if it prints "Batches to process 0"
# (demo-only) snapshot the post-A state to rehearse part 2 later, before the fiftyone stage:
docker exec <pg> pg_dump -U postgres -n "$DB_SCHEMA" postgres > /tmp/checkpoint.sql
datapipe step --labels=stage=fiftyone run         # GT + model-A predictions into FiftyOne
```

## Part 2 — retrain and watch the tag metric rise
```bash
python ../scripts/add_request.py --id night-train-a --n 50 --offset 350 --subset train --tag night --darken 0.30
python ../scripts/add_request.py --id night-train-b --n 50 --offset 400 --subset train --tag night --darken 0.40
python ../scripts/add_request.py --id night-train-c --n 50 --offset 450 --subset train --tag night --darken 0.55
datapipe step --labels=stage=load run
datapipe step --labels=stage=train run            # model B (night now in training)
datapipe step --labels=stage=count-metrics run    # re-run once if "0 batches"
datapipe step --labels=stage=fiftyone run         # adds predictions_model_b
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
  `predictions_model_b`; sample fields `tag_id` / `subset_id`. Launch:
  `fiftyone app launch "$FIFTYONE_DATASET_NAME" --port 5151 --address 0.0.0.0` (tunnel that port if
  remote — local port must equal the remote port).

### Pre-staged cache (fast loads)
If `DATAPIPE_TAGS_CACHE_DIR/gt.json` + `DATAPIPE_TAGS_CACHE_DIR/images/<file>.jpg` exist, the load
step reads from them instead of fetching COCO. `.env` sets `DATAPIPE_TAGS_CACHE_DIR` (default
`/tmp/datapipe-tags-cache`).

On a COCO-reachable host, build the cache once from `examples/detection_tags/detection`:

```bash
python ../scripts/build_cache.py 500   # or a larger N; keep batch totals ≤ cache size
```

The cache caps the image pool at its size, so keep the batch total ≤ it.

## Notes
- Classes are lowercase (`cat`/`dog`) to match COCO so injected GT and predictions align.
- Trust `detection_training_status.status`, not exit codes; `count-metrics` may need a second run.
- FiftyOne integration matches `examples/e2e_template/image_detection` (base `fiftyone_images` store + overlays).
