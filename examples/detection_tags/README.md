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

## Two-step data load (no annotation)
```bash
# from examples/detection_tags/detection
python ../scripts/load_batch.py --n 120                       # batch 1: base cat/dog
python ../scripts/load_batch.py --n 40 --tag night --darken 0.1   # batch 2: tagged low-light scenario
```
`load_batch.py` downloads COCO cat/dog images, uploads them to MinIO, and injects ground truth
(+ a tag for batch 2). No human labelling.

## Run the pipeline
```bash
datapipe run                    # ingest -> split -> freeze -> train -> inference -> metrics -> tag_metrics
# or by stage: datapipe step --labels=stage=train run
```

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
- Tables `tag` / `image__tag` are external inputs (written by `load_batch.py`); `tag_metrics` is
  produced by a pipeline `BatchTransform` (`steps.compute_tag_metrics`).
