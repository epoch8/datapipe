# Output Cleanup and `processed_idx`

When a `BatchTransform` writes results, Datapipe does not blindly append rows. Each output `store_chunk` receives a **`processed_idx`**: the set of output primary keys that this batch was responsible for. Any existing rows in that index that are **missing from the returned DataFrame** are removed.

This is how Datapipe keeps 1-to-N (and partial-update) outputs consistent without reprocessing the whole table.

## Mental model

```text
BatchTransform runs for transform key K
        │
        ▼
func returns DataFrame D (may have 0, 1, or many rows per K)
        │
        ▼
store_chunk(D, processed_idx = mapped keys from batch idx)
        │
        ├── rows in D           → insert / update (by PK)
        └── keys in processed_idx but not in D  → hard-delete data + soft-delete meta
```

For a single 1→1 step, `processed_idx` is usually just the batch's transform index mapped to the output table's primary keys. For 1→N, it is still the **parent** transform key — not every child row — so omitted children disappear on the next successful run.

## Where it comes from

You rarely pass `processed_idx` yourself in a transform. `BatchTransformStep.store_batch_result` maps the batch `idx` to each output spec via `_transform_idx_to_output_idx` and passes that into `DataTable.store_chunk`.

If your function returns **`None`** (typical delete path), Datapipe skips `store_chunk` and calls `delete_by_idx` for existing output keys in the batch instead — same end state, different code path.

## 1-to-N example

Parent table `products` keyed by `(pipeline_id, offer_id)`. Child table `attributes` keyed by `(pipeline_id, offer_id, name)`.

When `(1, 42)` is dirty, the transform runs once for that parent key. It might yield three attribute rows. On the next run the same parent key might yield only two — the third child row is deleted because its composite key falls under the batch's `processed_idx` but is absent from the result.

See [How to Expand One Row Into Many](../how-to/one-to-many.md) and the animated walkthrough below.

![Partial batch output deletes missing B rows](../assets/incremental/05-processed-idx.gif)

## Tips & pitfalls

| Pitfall | What goes wrong | What to do |
|---|---|---|
| **Partial return without thinking about cleanup** | Old child rows linger forever | Always return the full desired set for the batch's transform keys, or rely on `processed_idx` cleanup intentionally |
| **Silent data loss** | You omit rows you meant to keep; Datapipe deletes them | Treat every successful run as "replace outputs for this idx slice" |
| **Wrong output PK / `OutputSpec` mapping** | `processed_idx` columns do not overlap output PKs → cleanup disabled | Align output primary keys and `OutputSpec` key maps with transform keys |
| **Empty DataFrame vs `None`** | Empty frame still runs `store_chunk` with `processed_idx` → deletes all outputs for the idx; `None` (no `idx` param) uses the explicit delete path | For "delete all children of this parent", either return an empty frame with correct columns or let the delete path run |
| **Manual `store_chunk` in app code** | No automatic `processed_idx` unless you pass it | When writing from outside a transform, pass `processed_idx` explicitly if you need the same delete semantics |

`processed_idx` is ignored when it shares **no columns** with the table's primary keys — in that case Datapipe will not delete "missing" rows, which can look like stale data mysteriously persisting.

## See also

- [Incremental Processing](./incremental-processing.md) — scheduling and the four base cases
- [Soft Delete](./soft-delete.md) — what happens in meta when rows are removed
- [Primary Keys and Transform Keys](./primary-keys.md) — transform grain vs output PK
- [Change Detection and Merging](../explanation/change-detection.md) — deleted / resurrected classification
- [`DataTable.store_chunk`](../reference/table.md) — API reference
