# Soft Delete vs Hard Delete

Datapipe splits "delete" into two layers. The **data store** loses the row immediately; the **meta plane** keeps the primary key alive with a tombstone so incremental scheduling still works.

## Two layers

| Layer | On delete | On read (default) | Purpose |
|---|---|---|---|
| **Data store** (TableStore) | **Hard delete** — row removed | Row gone | Actual payload storage |
| **Table meta** (`{table}_meta`) | **Soft delete** — `delete_ts` set, `hash = 0`, `update_ts` bumps | Filtered out (`delete_ts IS NULL`) | Change detection + downstream scheduling |

```text
delete_by_idx / store_chunk cleanup
        │
        ├── TableStore     → DELETE row (hard)
        └── {table}_meta   → delete_ts = now, update_ts = now, hash = 0 (soft)
```

Downstream `BatchTransform` steps do **not** poll the data store for missing parents. They watch **`update_ts`** on input meta. A soft-deleted key still has a meta row with a fresh `update_ts`, so the step schedules, loads empty input data, and cleans its outputs.

## Delete propagation (A → B)

1. Key disappears from A's data store; A_meta gets `delete_ts`.
2. `update_ts` on A_meta moves forward → transform key is dirty.
3. Step runs; `get_batch_input_dfs` returns empty frames for that key.
4. Without an `idx` parameter, `func` is **not** called; Datapipe returns `None` and deletes B rows for that index.

See the delete GIF in [Incremental Processing](./incremental-processing.md#3-delete--data-disappears).

## Resurrection

Writing a row again under the same primary key after a soft delete is a **resurrection**:

- Data store: insert (or update) as usual.
- Meta: `delete_ts` cleared, `hash` recomputed, `update_ts` bumps — treated like new/changed content.
- Downstream steps schedule again and can rebuild outputs.

![Soft delete then undelete resurrects the key](../assets/incremental/06-resurrection.gif)

Resurrection is the mirror of delete: meta drives scheduling even when the key was "gone" from live reads.

## Downstream scheduling after delete

Soft delete intentionally **does not** remove the transform key from meta. That is a feature:

- Steps that already processed the key have `process_ts` in the past relative to the bumped `update_ts`.
- The empty-input batch runs once to propagate cleanup.
- After success, `{step}_meta.process_ts` catches up until the next change.

If you only hard-deleted data without touching meta (bypassing Datapipe APIs), downstream steps would **not** schedule — stale outputs would remain. Always delete through `DataTable.delete_by_idx`, `store_chunk(..., processed_idx=...)`, or transform output paths.

## Tips & pitfalls

| Pitfall | Symptom | Fix |
|---|---|---|
| **Direct SQL / file delete bypassing meta** | B still has rows; step never re-runs | Use Datapipe delete APIs or `UpdateExternalTable` to sync meta |
| **Assuming soft-deleted keys are gone from scheduling** | Surprise empty-input runs | Expect one cleanup batch per deleted transform key |
| **Confusing `process_ts` with `update_ts`** | Same-hash rewrite does not schedule; soft delete does | Scheduling uses input `update_ts`, not table `process_ts` |
| **Resurrection without downstream step** | A live again but B empty until next pipeline run | Normal — run the pipeline or rely on changelist propagation |
| **Reading meta with deleted rows** | Counts look inflated | Live reads filter `delete_ts IS NULL`; use `include_deleted=True` only when debugging |

## See also

- [Incremental Processing](./incremental-processing.md) — delete case and scheduling rule
- [Output Cleanup and `processed_idx`](./processed-idx.md) — removing output rows on partial batches
- [The `idx` Parameter](./idx-parameter.md) — empty inputs with vs without `idx`
- [Meta-Table Schema](../explanation/meta-table-schema.md) — column definitions
- [Change Detection and Merging](../explanation/change-detection.md) — resurrected / deleted outcomes
