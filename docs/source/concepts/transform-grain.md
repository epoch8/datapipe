# Transform Grain

A `BatchTransform` does not schedule work row-by-row on every table. It schedules at a **transform grain**: the set of columns in **`transform_keys`** that define one unit of work — one batch index, one call to `func`, one row (or tuple of rows) in `{step}_meta`.

Getting the grain wrong is the most common reason pipelines "re-run everything" or "never join inputs correctly."

## Default grain

If you omit `transform_keys`, Datapipe infers them from the intersection of input and output primary keys. When all tables share `(word_id)`, the grain is `word_id`. When child outputs add columns (1-to-N), the shared parent keys become the grain.

Override explicitly whenever:

- Multiple input tables contribute different key columns.
- The natural batch is a **cross product** (model × image).
- Input PK names differ (`Required` / key mapping).

## Multi-input: `max(update_ts)` per input

Each input table is aggregated to the transform keys it shares with the step. Datapipe takes **`max(update_ts)`** per group — the latest change signal for that input at this grain.

Those aggregates are joined into one row per transform key. The combined input timestamp is the **greatest** of the per-input maxima:

```text
dirty input signal = max( max(update_ts) over input₁, max(update_ts) over input₂, … )
```

Compared against `{step}_meta.process_ts`, this decides whether the transform key runs.

## Cross-product joins

When two inputs share **no** primary-key columns with each other (only with `transform_keys` via separate columns), the aggregate CTEs join on `TRUE` — a **full cross product** at the transform grain.

Example: `models(model_id)` and `images(image_id)` with `transform_keys=["model_id", "image_id"]` → one work unit per `(model_id, image_id)` pair. Updating one model re-dirties every pair involving that model.

```mermaid
flowchart LR
  subgraph inputs
    M[models_meta<br/>GROUP BY model_id<br/>max update_ts]
    I[images_meta<br/>GROUP BY image_id<br/>max update_ts]
  end
  M --> X["FULL JOIN ON TRUE<br/>(cross product)"]
  I --> X
  X --> G["greatest(m_ts, i_ts)<br/>per model_id, image_id"]
  G --> J["FULL OUTER JOIN<br/>step_meta"]
  J --> D{update_ts > process_ts<br/>OR missing / failed?}
  D -->|yes| RUN[Schedule batch]
  D -->|no| SKIP[Skip key]
```

## `InputSpec`, `OutputSpec`, and key mapping

Plain table names assume matching primary keys. When names or grains differ, use specs:

```python
BatchTransform(
    infer,
    inputs=[
        "models",
        Required("images", join_keys={"image_id": "image_id"}),
    ],
    outputs=[
        OutputSpec("predictions", output_keys={"model_id": "model_id", "image_id": "image_id"}),
    ],
    transform_keys=["model_id", "image_id"],
)
```

- **`InputSpec` / `Required`** — how an input table attaches to transform keys (including renames).
- **`OutputSpec`** — how batch `idx` maps to output primary keys for `processed_idx` cleanup.

Misaligned specs break joins in the scheduling SQL or disable output cleanup.

## Relationship to `processed_idx`

Transform grain = scheduling + batching. **`processed_idx`** = output cleanup scope for that batch, mapped through `OutputSpec` to each output table's PKs. Same parent grain can map to many child rows; see [Output Cleanup and `processed_idx`](./processed-idx.md).

## Tips & pitfalls

| Pitfall | Symptom | Fix |
|---|---|---|
| **Implicit grain on multi-input** | Wrong keys inferred; cartesian explosion or no matches | Set `transform_keys=` explicitly |
| **Cross product surprise** | One model change re-runs all image pairs | Expected — narrow inputs, filters, or redesign grain |
| **`inner` vs full join on inputs** | Keys missing from one input never schedule | Understand `join_type` per input in agg CTEs |
| **Child table as input without parent keys in grain** | Over- or under-scheduling | Include parent keys in `transform_keys` |
| **Mismatched `OutputSpec` keys** | Stale output rows never deleted | Align `output_keys` with transform and output PKs |

## See also

- [Primary Keys and Transform Keys](./primary-keys.md) — PK vs transform key basics
- [Incremental Processing](./incremental-processing.md) — dirty predicate overview
- [Change Detection and Merging](../explanation/change-detection.md) — multi-input SQL detail
- [Key Mapping](../how-to/key-mapping.md) — practical join patterns
- [Model Inference](../how-to/model-inference.md) — cross-product example
