# The `idx` Parameter

`BatchTransform` can inject an **`idx`** argument: the current batch's transform keys as an `IndexDF`. Declaring it is optional — but it changes how **delete** and **empty-input** batches behave.

## Without `idx` (default delete path)

When your function does **not** declare `idx` and every input DataFrame for the batch is empty (typical after a soft-deleted input key):

1. Datapipe **does not call** `func`.
2. `process_batch_dts` returns **`None`** immediately.
3. `store_batch_result` deletes existing output rows for that batch index and marks transform meta successful.

This is the fast path for propagating deletes downstream — your code never runs.

```text
empty inputs ∧ no idx param  →  None  →  delete_by_idx(outputs)  →  step_meta OK
```

See [Soft Delete](./soft-delete.md) and the delete panels in [Incremental Processing](./incremental-processing.md).

## With `idx` (explicit empty batch)

If `idx` appears in the function signature, Datapipe **always calls** `func`, even when all input frames are empty. You receive the batch index and empty DataFrames — and must decide what to return.

| Return value | Effect |
|---|---|
| **`None`** | Same as default delete path — outputs for this idx removed |
| **Empty DataFrame** (correct columns) | `store_chunk` with `processed_idx` — deletes all output rows for this idx |
| **Non-empty DataFrame** | Upsert those rows; keys under `processed_idx` but missing from the frame are deleted |

Use `idx` when you need side effects on delete (logging, external API calls, conditional retention) or when empty inputs still carry meaning.

## Example signatures

```python
# Default — delete path automatic
def count_chars(words: pd.DataFrame) -> pd.DataFrame:
    return words.assign(n=words["text"].str.len())


# Explicit — func runs on delete
def maybe_retain(words: pd.DataFrame, idx: pd.DataFrame) -> pd.DataFrame | None:
    if words.empty:
        return None  # still delete outputs
    return words.assign(n=words["text"].str.len())
```

Magic kwargs (`ds`, `run_config`) compose with `idx` — only declare what you need.

## Interaction with soft delete

Soft-deleted input keys still schedule the step. Without `idx`, cleanup is automatic. With `idx`, you see empty inputs and can branch — but returning nothing useful when you meant to delete outputs will still remove B if you return `None` or an empty frame with proper columns.

## Tips & pitfalls

| Pitfall | What happens | What to do |
|---|---|---|
| **Adding `idx` "for debugging"** | Delete path stops skipping `func`; empty-input batches now hit your code | Remove `idx` unless you need custom delete logic |
| **Empty frame with wrong columns** | `store_chunk` may error or mis-clean | Match output table schema even for empty results |
| **Assuming `func` never runs on delete** | Breaks when `idx` is present | Document team convention for delete returns |
| **Returning stale rows on empty input** | Unexpected output retention | Return `None` or empty frame to trigger cleanup |
| **Forgetting `processed_idx` semantics** | Partial empty returns delete missing output keys | See [Output Cleanup and `processed_idx`](./processed-idx.md) |

## See also

- [Incremental Processing](./incremental-processing.md) — "What your function sees" table
- [Soft Delete](./soft-delete.md) — why inputs are empty but keys still schedule
- [Compute Step Lifecycle](../explanation/compute-step-lifecycle.md) — `process_batch_dts` branch
- [BatchTransform](../reference/steps/batch-transform.md) — injected kwargs reference
