# Pipeline Steps

A pipeline is an ordered list of **PipelineStep** objects. At build time each step expands into one or more **ComputeStep** nodes (the runtime graph).

```python
pipeline = Pipeline([
    BatchGenerate(generate_words, outputs=["words"]),
    BatchTransform(count_chars, inputs=["words"], outputs=["word_lengths"]),
])
```

## Which step type?

| Step | Incremental? | Use when |
|---|---|---|
| [`BatchGenerate`](../reference/steps/batch-generate.md) | Seeds outputs | Source tables / generators (`yield` DataFrames) |
| [`UpdateExternalTable`](../reference/steps/update-external-table.md) | Syncs meta | Data written outside Datapipe |
| [`BatchTransform`](../reference/steps/batch-transform.md) | **Yes** (row-level) | Stateless DataFrame→DataFrame work |
| [`DatatableTransform`](../reference/steps/datatable-transform.md) | No (whole tables) | Global jobs (e.g. full training) needing `DataTable` handles |

## Data-flow patterns

### Generate from outside

Pull a feed, list S3 keys, call an API — emit batches with `BatchGenerate`, or refresh meta with `UpdateExternalTable` if another process owns the files/rows.

### 1-to-1 batch transform

Resize images, normalize text, run per-row enrichment. Classic `BatchTransform`. Example: `examples/datapipe_core/image_resize/`.

### 1-to-N or N-to-1 on chunks

Expand attributes or collapse boxes. Still `BatchTransform`; watch `transform_keys` and output `processed_idx` semantics. Example: `examples/datapipe_core/one_to_many_pipeline/`.

### Multi-input

Model weights × images → predictions. Set `transform_keys` to the product grain. Example: `examples/datapipe_core/model_inference/`.

### Global / near-global

Training that must see “all” data — prefer `DatatableTransform` (no per-row changelist). Accept that Datapipe will not skip unchanged rows for you.

## Labels and filters

Steps can carry `labels` for CLI selection (`datapipe step run --labels …`) and `filters` to restrict which rows participate. See [Filter Steps by Labels](../how-to/filter-by-labels.md).

## Magic kwargs on `BatchTransform` / `BatchGenerate`

If your function declares these parameters, Datapipe injects them:

| Parameter | Injected value |
|---|---|
| `ds` | Active `DataStore` |
| `idx` | `IndexDF` for the current batch |
| `run_config` | Current `RunConfig` |

Declaring `idx` also changes delete behaviour: empty inputs still call `func` instead of the automatic `None` → cleanup path.

## See also

- [Incremental Processing](./incremental-processing.md) — when BatchTransform re-runs
- [Compute Step Lifecycle](../explanation/compute-step-lifecycle.md)
- [Steps reference](../reference/steps/index.md)
