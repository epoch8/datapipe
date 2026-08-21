# Primary Keys and Transform Keys

Incremental scheduling is **key-based**. Getting keys right is the difference between “only changed rows re-run” and “everything always re-runs” (or never joins).

## Primary keys (table)

Each table declares a primary key — one or more columns that uniquely identify a row. Meta rows use the same key columns.

```python
class Word(Base):
    __tablename__ = "words"
    word_id: Mapped[int] = mapped_column(primary_key=True)
    text: Mapped[str]
```

When you `store_chunk` a DataFrame, those PK columns must be present.

## Transform keys (step)

A `BatchTransform` works on a **transform key** — the grain at which Datapipe decides “this unit of work is dirty.”

- Default: intersection of input (and output) primary keys when they align.
- Override with `transform_keys=["col_a", "col_b"]` when the natural grain differs (e.g. inference over `model_id × image_id`).

Dirty detection aggregates each input by the overlap of that input’s PKs with the transform keys, then compares `max(update_ts)` to the step’s `process_ts`.

## When keys differ across tables

Use `Required(...)` / input–output specs so Datapipe knows how to join. Patterns:

| Pattern | Example | Docs |
|---|---|---|
| Same PK 1→1 | `word_id` → `word_id` | [Transform Files](../how-to/transform-files.md) |
| 1→N expand | one product → many attributes | [One-to-Many](../how-to/one-to-many.md) |
| Mismatched names / joins | `id` vs `image_id` | [Key Mapping](../how-to/key-mapping.md) |
| Cross product grain | models × images | [Model Inference](../how-to/model-inference.md) |

Deep design notes: `libs/datapipe-core/design-docs/2025-12-key-mapping.md`.

## Practical rules

1. Prefer **stable business keys** over autoincrement surrogate keys that change meaning.
2. Set `transform_keys` explicitly whenever more than one input table is involved.
3. Remember: deletes soft-keep the key in meta — the transform key still appears in the dirty set.
4. Output cleanup uses the batch’s transform idx as `processed_idx` when writing results.

## See also

- [Incremental Processing](./incremental-processing.md)
- [Types](../reference/types.md) — `Required`, `InputSpec`, `OutputSpec`
- Example: `examples/datapipe_core/key_mapping/`
