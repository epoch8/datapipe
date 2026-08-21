# How to Filter Steps by Labels

Select a subset of pipeline steps from the CLI, and optionally restrict which rows a transform processes.

## Goal

Tag steps with labels, then run or inspect only matching steps — useful for staging, backfills, or isolating expensive work.

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
datapipe step --name=enrich --labels=stage=enrich run
```

The same `--labels` / `--name` filters apply to `step list`, `step run`, and `step reset-metadata`.

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

## Expected result

- `datapipe step --labels=… list` shows only tagged steps.
- `run` / `reset-metadata` affect that subset only; other steps stay untouched.
- With `filters`, dirty-key discovery and processing stay within the filtered key values.

## See also

- [CLI Commands](../reference/cli.md)
- [Pipeline Steps](../concepts/pipeline-steps.md)
