# Change Detection and Merging

Datapipe decides what changed by comparing **content hashes** and **timestamps** in meta tables — not by diffing full payloads. Two layers cooperate:

1. **Table meta** (`{table}_meta`) — per primary-key row fingerprint and lifecycle.
2. **Transform meta** (`{step}_meta`) — per transform-key last successful (or failed) run.

For the animated walkthrough of insert / update / delete / unchanged, see [Incremental Processing](../concepts/incremental-processing.md).

## Table meta fields

Defined by `TABLE_META_SCHEMA` in `datapipe.meta.sql_meta`:

| Field | Meaning |
|---|---|
| `hash` | Content fingerprint of the row (from `TableStore.hash_rows`). Same values → same hash. |
| `create_ts` | When the meta row was first created. |
| `update_ts` | When content last **changed** (or was soft-deleted / resurrected). Identical rewrites do **not** bump this. |
| `process_ts` | When this row was last touched by a successful `store_chunk` (including unchanged rewrites). |
| `delete_ts` | Soft-delete marker. `NULL` means the row is live. Non-null means data was removed from the store but meta keeps the key. |

**Scheduling downstream steps** depends on `update_ts`, not `process_ts`. Bumping only `process_ts` (same hash) does not dirty consumers.

## `get_changes_for_store_chunk`

On every write, `DataTable.store_chunk` hashes the incoming DataFrame and calls `SQLTableMeta.get_changes_for_store_chunk`. It left-joins the new hashes against existing meta (including deleted rows) and classifies each primary key:

| Outcome | Condition | Data store | Meta |
|---|---|---|---|
| **New** | No meta row yet (`hash_exist` is null) | `insert_rows` | Insert: set `hash`, `create_ts`, `update_ts`, `process_ts`; `delete_ts = NULL` |
| **Changed** | Meta exists, not deleted, `hash ≠ hash_exist` | `update_rows` | Bump `update_ts` and `process_ts`; clear `delete_ts` |
| **Unchanged** | Meta exists, not deleted, same hash | **Skip** data rewrite | Still set `process_ts = now`; leave `update_ts` alone |
| **Resurrected** | Meta exists with `delete_ts` set | Treated like insert (`insert_rows`) | Clear `delete_ts`; bump `update_ts` and `process_ts` |
| **Deleted** | Key in `processed_idx` but missing from `data_df` | Hard-delete from store | `mark_rows_deleted`: `hash = 0`, set `delete_ts`, bump `update_ts` and `process_ts` |

Deleted keys are **not** returned from `get_changes_for_store_chunk` itself — `store_chunk` computes them via `processed_idx` and calls `delete_by_idx` / `mark_rows_deleted`.

Conceptually:

```text
new          ⇔  no meta yet
changed      ⇔  live meta ∧ hash differs
unchanged    ⇔  live meta ∧ hash same
resurrected  ⇔  soft-deleted meta ∧ key written again
deleted      ⇔  in processed_idx ∧ absent from chunk
```

Returned indexes drive inserts vs updates; returned meta frames drive the upsert into `{table}_meta`.

## Four cases (A → B)

The single-key pipeline cases map directly onto the table above. GIFs and a function-level view live in the concepts page:

1. **Insert** — [01-insert](../concepts/incremental-processing.md#1-insert--new-data-appears)
2. **Update** — [02-update](../concepts/incremental-processing.md#2-update--content-changes)
3. **Delete** — [03-delete](../concepts/incremental-processing.md#3-delete--data-disappears)
4. **Unchanged** — [04-unchanged](../concepts/incremental-processing.md#4-unchanged--rewrite-with-the-same-content)

## Transform scheduling SQL

A `BatchTransform` finds dirty transform keys in `_build_changed_idx_sql` (via `get_full_process_ids` / changelist variants). Conceptually:

1. For each input, build a CTE that aggregates `max(update_ts)` at the intersection of that table’s primary keys and the step’s `transform_keys` (`get_agg_cte`).
2. Combine those CTEs into one aggregate-of-aggregates (`_make_agg_of_agg`): join on shared transform keys; if inputs share no keys, join on `TRUE` (product / cartesian). `join_type` is `full` or `inner` per input. The combined `update_ts` is the greatest of the per-input maxima.
3. Full outer-join that aggregate with the transform meta table on `transform_keys`.
4. Keep rows that are dirty:

```text
dirty  ⇔  (is_success ∧ input.update_ts > step.process_ts)
       OR  is_success is not true
       OR  step.process_ts IS NULL
```

Order prefers higher `priority`, then transform keys (or an explicit `order_by`).

Minimal single-input shape:

```sql
WITH input__update AS (
    SELECT id, max(update_ts) AS update_ts
    FROM a_meta
    GROUP BY id
),
transform AS (
    SELECT id, process_ts, priority, is_success
    FROM step_meta
)
SELECT coalesce(i.id, t.id) AS id
FROM input__update i
FULL OUTER JOIN transform t ON i.id = t.id
WHERE
    (t.is_success IS TRUE AND i.update_ts > t.process_ts)
    OR t.is_success IS NOT TRUE
    OR t.process_ts IS NULL
ORDER BY t.priority DESC NULLS LAST, id;
```

## Multi-input join strategy

Example: inputs `models` (`model_id`) and `images` (`image_id`); transform and output keyed by `(model_id, image_id)` — e.g. model inference per image.

Strategy:

1. Aggregate each input by the transform keys it contributes (`model_id` or `image_id`), taking `max(update_ts)`.
2. Join those aggregates into one CTE at transform grain. With no shared keys this is a full/cross join — every model paired with every image — and `update_ts = greatest(models.update_ts, images.update_ts)`.
3. Full outer-join with `{step}_meta` on `(model_id, image_id)`.
4. Apply the dirty predicate above.

Illustrative SQL (matches the idea of the runtime CTEs, not a literal dump):

```sql
WITH models__update AS (
    SELECT model_id, max(update_ts) AS update_ts
    FROM models_meta
    GROUP BY model_id
),
images__update AS (
    SELECT image_id, max(update_ts) AS update_ts
    FROM images_meta
    GROUP BY image_id
),
all__update_ts AS (
    SELECT
        coalesce(m.model_id) AS model_id,
        coalesce(i.image_id) AS image_id,
        greatest(m.update_ts, i.update_ts) AS update_ts
    FROM models__update m
    FULL OUTER JOIN images__update i ON TRUE
),
transform AS (
    SELECT model_id, image_id, process_ts, priority, is_success
    FROM model_inference_meta
)
SELECT
    coalesce(a.model_id, t.model_id) AS model_id,
    coalesce(a.image_id, t.image_id) AS image_id
FROM all__update_ts a
FULL OUTER JOIN transform t
    ON a.model_id = t.model_id AND a.image_id = t.image_id
WHERE
    (t.is_success IS TRUE AND a.update_ts > t.process_ts)
    OR t.is_success IS NOT TRUE
    OR t.process_ts IS NULL;
```

Changing either a model or an image bumps the combined `update_ts` for every affected `(model_id, image_id)` pair, so those tasks re-run. Keys that only exist on the transform side (inputs gone) still appear via the full outer join and stay dirty until cleaned up.

## See also

- [Incremental Processing](../concepts/incremental-processing.md) — four cases with GIFs
- [Meta-Table Schema](./meta-table-schema.md) — column definitions
- [Compute Step Lifecycle](./compute-step-lifecycle.md) — how batches run once indexes are selected
- [Primary Keys and Transform Keys](../concepts/primary-keys.md)
