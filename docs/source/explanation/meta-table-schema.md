# Meta-Table Schema

Datapipe stores bookkeeping in SQL meta tables next to (or in the same database as) your data. Schemas are defined in `datapipe.meta.sql_meta` as `TABLE_META_SCHEMA` and `TRANSFORM_META_SCHEMA`.

## Table meta — `{name}_meta`

Each `DataTable` gets a meta table named `{name}_meta`. Physical columns are:

```text
primary_schema  +  optional meta_schema  +  TABLE_META_SCHEMA
```

`TABLE_META_SCHEMA`:

| Column | Type | Role |
|---|---|---|
| `hash` | `Integer` | Content fingerprint of the data row. |
| `create_ts` | `Float` | Unix time when the meta row was first inserted. |
| `update_ts` | `Float` | Unix time of the last content change (or soft delete / resurrection). |
| `process_ts` | `Float` | Unix time of the last successful `store_chunk` touch for this key. |
| `delete_ts` | `Float` | Soft-delete time; `NULL` while the row is live. |

Primary-key columns come from the table’s `primary_schema` and are marked as the SQL primary key. Optional `meta_schema` columns (e.g. `MetaKey` helpers) sit between PKs and the fixed lifecycle fields.

Reads of “existing” indexes typically filter `delete_ts IS NULL`. Soft-deleted rows remain queryable when change detection needs them (`include_deleted=True`), so a later write can resurrect the same primary key.

Implemented by `SQLTableMeta`.

## Transform meta — `{step}_meta`

Each incremental compute step (e.g. `BatchTransformStep`) creates a transform meta table, usually named `{step_name}_meta`. Columns are:

```text
transform_keys_schema  +  TRANSFORM_META_SCHEMA
```

`TRANSFORM_META_SCHEMA`:

| Column | Type | Role |
|---|---|---|
| `process_ts` | `Float` | Unix time of the last attempt that recorded a result for this transform key. |
| `is_success` | `Boolean` | `True` after `store_batch_result`; `False` after `store_batch_err`. |
| `priority` | `Integer` | Scheduling priority; higher values are processed first (`NULL`s last). |
| `error` | `String` | Error text when `is_success` is false; cleared on success. |

`transform_keys_schema` is computed from the step’s inputs/outputs (and optional explicit `transform_keys`). Those key columns are the grain at which Datapipe decides “this task needs to run.”

Implemented by `SQLTransformMeta`.

## How the two schemas connect

```text
{table}_meta.update_ts     ──►  compared to  ──►  {step}_meta.process_ts
{table}_meta.hash          ──►  detects content change on store
{step}_meta.is_success     ──►  failed / missing keys stay dirty
```

A transform key is selected when input `update_ts` is newer than the step’s `process_ts`, or when the step has never succeeded for that key. Details: [Change Detection and Merging](./change-detection.md).

## See also

- [Incremental Processing](../concepts/incremental-processing.md)
- [Tables and Stores](../concepts/tables-and-stores.md)
- [Change Detection and Merging](./change-detection.md)
