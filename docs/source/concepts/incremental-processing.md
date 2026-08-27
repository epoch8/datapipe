# Incremental Processing

Incremental processing is Datapipe's core feature. You write plain transform functions; Datapipe decides **which keys** need work on each run — and skips the rest.

```text
A (input) ──BatchTransform──► B (output)
     │                              ▲
     └── metadata tracks what changed
```

Before the deep dive, watch the four cases below. They use a single-key A→B pipeline.

## The four cases

### 1. Insert — new data appears

![Insert: new row schedules the transform](../assets/incremental/01-insert.gif)

A new row is written to A. Meta gets a fresh `hash` and `update_ts`. The transform has no `process_ts` for that key yet, so it is selected. Your function runs; B is written.

### 2. Update — content changes

![Update: hash change bumps update_ts and re-runs](../assets/incremental/02-update.gif)

Same primary key, different content → different CityHash. `update_ts` moves forward. Because `update_ts > process_ts`, the key is scheduled again. B is updated for that key only.

### 3. Delete — data disappears

![Delete: soft meta delete propagates to B](../assets/incremental/03-delete.gif)

Data is **hard-deleted** from the store. Meta keeps the key with `delete_ts` set and bumps `update_ts` (soft delete). The step still runs. With empty inputs (and no `idx` parameter on `func`), Datapipe returns `None` and cleans B for that index. See [Soft Delete](./soft-delete.md) for the two-layer model and [The `idx` Parameter](./idx-parameter.md) when your function declares `idx`.

### 4. Unchanged — rewrite with the same content

![Unchanged: same hash skips the transform](../assets/incremental/04-unchanged.gif)

Writing the same values again matches the existing `hash`. **Data is not rewritten.** `update_ts` does **not** move (A_meta `process_ts` may still bump). The step is **not** scheduled. Your function never runs. This is the incremental win.

## Metadata legend

| Store | Fields that matter | Role |
|---|---|---|
| `{table}_meta` | `hash`, `update_ts`, `process_ts`, `delete_ts` | Per-row content fingerprint and lifecycle |
| `{step}_meta` | `process_ts`, `is_success` | Per-key last successful (or failed) transform run |

**Scheduling rule (conceptually):** a key is dirty when input `update_ts` is newer than the step's `process_ts`, or when the step has never succeeded for that key.

```text
dirty  ⇔  max(input.update_ts) > step.process_ts
       OR  step row missing / is_success is false
```

Identical rewrites bump only table `process_ts`, not `update_ts` — so they do **not** dirty the step.

## What your function sees

| Case | Scheduled? | `func` called? | Input | Output effect |
|---|---|---|---|---|
| Insert | Yes | Yes | New rows | Insert into B |
| Update | Yes | Yes | Updated rows | Update B |
| Delete | Yes | Usually no* | Empty | Delete B for idx |
| Same hash | No | No | — | None |

\*If `func` declares an `idx` parameter, it is still called with empty DataFrames on delete — see [The `idx` Parameter](./idx-parameter.md).

You do **not** write change-detection logic. Receive a `pd.DataFrame` batch, return a `pd.DataFrame` (or tuple). Datapipe owns the index set.

## Advanced cases

### Output cleanup (`processed_idx`)

When a transform returns only part of the output rows for a batch, Datapipe deletes keys that fall under the batch index but are missing from the result. Common in 1-to-N pipelines — easy to cause **silent data loss** if you omit rows unintentionally.

![Partial batch output deletes missing B rows](../assets/incremental/05-processed-idx.gif)

→ [Output Cleanup and `processed_idx`](./processed-idx.md)

### Resurrection

Re-inserting a row under the same primary key after soft delete clears `delete_ts`, bumps `update_ts`, and schedules downstream steps again.

![Soft delete then undelete resurrects the key](../assets/incremental/06-resurrection.gif)

→ [Soft Delete](./soft-delete.md#resurrection)

## Full run vs changelist

- **`run` / `step run` (full):** SQL finds all dirty keys, processes in `chunk_size` batches.
- **`run_changelist`:** starts from an explicit `ChangeList` of indexes and can propagate through the graph for several iterations.

Both paths share the same `process_batch` implementation.

## Durability

After each successful batch, transform meta is updated. If the process crashes mid-run, the next run resumes from keys that are still dirty — no full replay required.

## See also

- [What is Datapipe?](./what-is-datapipe.md) — product overview
- [Soft Delete](./soft-delete.md) — hard vs soft delete, resurrection
- [Output Cleanup and `processed_idx`](./processed-idx.md) — 1-to-N and partial outputs
- [Transform Grain](./transform-grain.md) — `transform_keys`, multi-input scheduling
- [The `idx` Parameter](./idx-parameter.md) — delete path with vs without `idx`
- [BatchGenerate vs BatchTransform](./generate-vs-transform.md) — generator vs step meta
- [Primary Keys and Transform Keys](./primary-keys.md) — how keys join across tables
- [Change Detection and Merging](../explanation/change-detection.md) — SQL-level detail
- [Meta-Table Schema](../explanation/meta-table-schema.md) — column definitions
- [BatchTransform](../reference/steps/batch-transform.md) — API reference
