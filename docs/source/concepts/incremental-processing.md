# Incremental Processing

Incremental processing is Datapipe's core feature. You write plain transform functions; Datapipe decides **which keys** need work on each run — and skips the rest.

```text
A (input) ──BatchTransform──► B (output)
     │                              ▲
     └── metadata tracks what changed
```

Before the deep dive, study the four cases below. Each figure shows **Before → During → After** table states. **Amber** rows are the indexes being processed; teal = written; red = deleted; gray = untouched.

## The four cases

### 1. Insert — new data appears

![Insert: empty tables → id=1 active → B written](../assets/incremental/01-insert.png)

A new record shows up on A. Datapipe marks that index dirty, runs your function once, and writes B.  
*Internals:* fresh content fingerprint and timestamps; the step has never processed this key.

### 2. Update — content changes

![Update: only id=1 highlighted while id=2 stays idle](../assets/incremental/02-update.png)

Same key, different content → only that index is active → your function re-runs → only that key’s B row updates. Neighboring keys stay gray.  
*Internals:* fingerprint changes and the input is newer than the step’s last success.

### 3. Delete — data disappears

![Delete: id=1 gone from A → cleanup → gone from B](../assets/incremental/03-delete.png)

A loses the row; Datapipe still schedules that index and cleans the matching key on B.  
*Internals:* hard delete in the store + soft delete in meta. See [Soft Delete](./soft-delete.md) and [The `idx` Parameter](./idx-parameter.md).

### 4. Unchanged — rewrite with the same content

![Unchanged: no amber rows — function does not run](../assets/incremental/04-unchanged.png)

Same content rewritten → **no index is active** → your function is not called → B untouched. This is the incremental win.  
*Internals:* fingerprint matches; input timestamps do not make the step dirty.

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

![processed_idx: child c omitted from return → deleted](../assets/incremental/05-processed-idx.png)

→ [Output Cleanup and `processed_idx`](./processed-idx.md)

### Resurrection

Re-inserting a row under the same primary key after soft delete clears `delete_ts`, bumps `update_ts`, and schedules downstream steps again.

![Resurrection: soft-deleted id=1 re-inserted → transform runs again](../assets/incremental/06-resurrection.png)

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
