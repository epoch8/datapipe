# How to Filter Steps by Labels

Select a subset of pipeline steps from the CLI, and optionally restrict which rows a transform processes.

## Goal

Tag steps with labels, then run or inspect only matching steps — useful for staging, backfills, or isolating expensive work.

## Prerequisites

- A pipeline with steps tagged via the `labels=` argument
- `datapipe` CLI installed (`datapipe-core` or `datapipe-app`)

## Example repo

Real-world stage labels from the shipped examples:

### E2E image detection (`examples/e2e_template/image_detection/app.py`)

| Label | When to run |
|-------|-------------|
| `stage=annotation` | List S3 images, pre-annotate, Label Studio sync |
| `stage=train` | Split, freeze, train YOLO, inference, metrics |
| `stage=train-prepare` | Sub-label: freeze dataset only |
| `stage=count-metrics` | Recompute metrics without retraining |
| `stage=inference` | Run detector only |
| `stage=fiftyone` | Publish to FiftyOne |

```bash
cd examples/e2e_template/image_detection
set -a && source ../.env && set +a
uv run datapipe --executor RayExecutor step --labels=stage=annotation run
uv run datapipe --executor RayExecutor step --labels=stage=train run
uv run datapipe --executor RayExecutor step --labels=stage=fiftyone run
```

Full walkthrough: [E2E Image Detection Walkthrough](../getting-started/e2e-image-detection-walkthrough.md).

### Detection tags (`examples/detection_tags/detection/app.py`)

| Label | When to run |
|-------|-------------|
| `stage=load` | Fetch COCO batch, write GT + tags |
| `stage=train` | Freeze, train, infer |
| `stage=count-metrics` | Subset-level metrics |
| `stage=tag-metrics` | Per-tag metrics (sub-label of count-metrics) |
| `stage=fiftyone` | FiftyOne export |

```bash
cd examples/detection_tags/detection
set -a && source ../.env && set +a
uv run datapipe --executor RayExecutor step --labels=stage=load run
uv run datapipe --executor RayExecutor step --labels=stage=train run
uv run datapipe --executor RayExecutor step --labels=stage=count-metrics run
```

Full walkthrough: [Run Detection Tags Pipeline](./run-detection-tags-pipeline.md).

A step matches `--labels=stage=count-metrics` even if it also carries `stage=tag-metrics` — CLI matching requires **all** listed pairs to be present on the step, not an exact single label.

## Steps

### 1. Attach labels to steps

`labels` is a list of `(key, value)` pairs on `BatchTransform`, `BatchGenerate`, `UpdateExternalTable`, and related steps:

```python
BatchTransform(
    enrich,
    inputs=[Raw],
    outputs=[Enriched],
    labels=[("stage", "enrich"), ("team", "search")],
)

BatchTransform(
    train,
    inputs=[Enriched],
    outputs=[Model],
    labels=[("stage", "train")],
)
```

See [Pipeline Steps](../concepts/pipeline-steps.md).

### 2. List or run by label (CLI)

`--labels` accepts comma-separated `key=value` pairs. A step matches only if it has **all** listed pairs:

```bash
# only enrich stage
datapipe step --labels=stage=enrich list
datapipe step --labels=stage=enrich run

# require both labels
datapipe step --labels=stage=enrich,team=search run
```

Combine with `--name` for prefix matching on the step name:

```bash
datapipe step --labels=stage=enrich run
datapipe step --name=enrich --labels=stage=enrich run
```

The same `--labels` / `--name` filters apply to `step list`, `step run`, and `step reset-metadata`.

With Ray for parallel steps:

```bash
uv run datapipe --executor RayExecutor step --labels=stage=train run
```

See [Run Steps with RayExecutor](./ray-executor.md).

### 3. (Optional) Restrict rows with `filters`

Separately from CLI labels, a `BatchTransform` can limit which keys participate via `filters` — a dict (or callable returning a dict) of column → value, merged into `RunConfig.filters`:

```python
BatchTransform(
    enrich,
    inputs=[Raw],
    outputs=[Enriched],
    labels=[("stage", "enrich")],
    filters={"pipeline_id": 1},  # or lambda: {"pipeline_id": current_id()}
)
```

Use filters when the step should always process a slice of the key space. Use CLI `--labels` when you want to choose **which steps** run.

The e2e detection template leaves a commented `filters={"subset_id": "val"}` on `CountMetrics_Subset_PipelineModel` — uncomment to limit metrics to val rows only.

## Verify

List steps that would run for a label before executing:

```bash
uv run datapipe --executor RayExecutor step --labels=stage=annotation list
```

Expect only annotation-stage steps with pending batch counts. After a successful run:

```bash
uv run datapipe --executor RayExecutor step --labels=stage=annotation list
```

Pending batches should drop to 0 for steps whose inputs are unchanged.

## Expected result

- `datapipe step --labels=… list` shows only tagged steps.
- `run` / `reset-metadata` affect that subset only; other steps stay untouched.
- With `filters`, dirty-key discovery and processing stay within the filtered key values.

## See also

- [CLI Commands](../reference/cli.md)
- [Pipeline Steps](../concepts/pipeline-steps.md)
- [E2E Image Detection Walkthrough](../getting-started/e2e-image-detection-walkthrough.md)
- [Run Detection Tags Pipeline](./run-detection-tags-pipeline.md)
