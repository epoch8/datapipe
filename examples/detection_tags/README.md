# detection_tags

A minimal, self-contained datapipe detection example built around **tags** — with **no Label
Studio and no FiftyOne**. Ground truth is injected directly (COCO labels, lowercase `cat`/`dog`),
so the whole thing runs unattended from scratch.

The point: train a baseline, then add a **tagged scenario batch**, retrain, and watch the metric on
that tag rise — visible in a dedicated `tag_metrics` table (a real pipeline step).

## Deploy from scratch
```bash
cp .env.example .env && set -a && source .env && set +a
docker compose up -d            # postgres + minio only
uv sync                         # cu124 torch, polars-lts-cpu, pi-heif baked in
cd detection
datapipe db create-all
```

## Two-step data load — via datapipe steps (no annotation)
Loading is a real pipeline step (`stage=load`): add a request row, then run the step. It downloads
COCO cat/dog, uploads to MinIO, and produces `s3_images` + ground truth (+ a tag) — no human labelling.
```bash
# from examples/detection_tags/detection
python ../scripts/add_request.py --id base --n 120
datapipe step --labels=stage=load run          # batch 1: base cat/dog
datapipe step --labels=stage=train run         # -> model A (baseline, no tag in training)

python ../scripts/add_request.py --id night --n 40 --offset 120 --tag night --darken 0.1
datapipe step --labels=stage=load run          # batch 2: tagged low-light scenario
datapipe step --labels=stage=train run         # -> model B (tag in training)
datapipe step --labels=stage=count-metrics run # (re)compute metrics incl. tag_metrics
```
Or just `datapipe run` after adding request rows to run every stage.

### Fast data on a slow/blocked network (pre-staged cache)
The `load` step downloads images from COCO per request, which is slow on a high-latency or
throttled link. To avoid it, pre-stage a cache once from a fast machine:
`DATAPIPE_TAGS_CACHE_DIR/images/<file>.jpg` + `DATAPIPE_TAGS_CACHE_DIR/gt.json`
(`{ "<file>.jpg": {"bboxes": [[x1,y1,x2,y2]], "labels": ["cat"|"dog"]}, ... }`). If that cache is
present, the load step reads images + ground truth from it (no COCO fetches at all). Build it by
downloading the COCO cat/dog subset elsewhere and copying the folder to `DATAPIPE_TAGS_CACHE_DIR`
(default `/tmp/datapipe-tags-cache`).

## What you get
- `detection_model_train` — trained model(s).
- `detection_model_train__metrics_on_subset` — overall metrics per model.
- **`tag_metrics`** — `(detection_model_id, tag_id, subset_id)` → precision/recall/f1. Compare the
  baseline (trained before batch 2) vs the retrained model on `tag=night, subset=val`: the tag recall
  rises once the tagged batch is in training.

## Notes
- Classes are lowercase (`cat`/`dog`) to match COCO, so injected GT and predictions align.
- On a tiny validation set the "best epoch" pick can latch onto an early checkpoint; this example
  uses 50 epochs and a non-trivial batch size to keep metrics meaningful.
- Tables `tag` / `image__tag` are external inputs (produced by the `load` step); `tag_metrics` is
  produced by a pipeline `BatchTransform` (`steps.compute_tag_metrics`).
