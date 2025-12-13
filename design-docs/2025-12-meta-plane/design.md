Main idea: separate all metadata management to separate package so, that inteface between compute/execution-plane and meta would be relatively narrow and we could create alternative meta-plane implementation

The goal of this change would be to extract all metadata manipulation into a separate package: `datapipe.meta` and define an interface

What's the interface surface for meta?

- MetaTable - should contain everything that's required for working with meta data of a table
- TransformMetaTable - should contain everything that's required for working with meta data of batched (index-based) transform
- CONSIDER add MetaTransformStep or similar to consolidate all logic, not only technical storage of tasks status


MetaTable and TransformMetaTable should create a parallel hierarchy to Compute* entities, and Compute* entities should include Meta* entities where necessary

The responsibility of Meta* entities should be: tracking meta data, i.e. storing meta data about the data, querying meta data, computing which transform tasks should be performed given current metadata state

---

# MetaTable interface

## `get_changes_for_store_chunk`

given current hash_df compute which rows should be inserted, updated or deleted

TODO move processed_idx logic inside get_changes_for_store_chunk

CONSIDER merge update and insert logic, most of our storages do upsert anyways

## `update_rows` - apply changes computed by get_changes_for_store_chunk

## `mark_rows_deleted` - remove given rows

TODO merge update and delete to `apply_changes_for_store_chunk` so that flow would be `get_changes -> changes_snapshot -> apply_changes`


## `get_stale_idx` - find idx to be deleted by process_ts

CONSIDER add `process_ts` optionally only for tables that really need it, because it is an extra column which might not exist and might not require updates, something like `track_process_ts` flag in constructor

---

# TransformMetaTable

DONE add metatable links for inputs and outputs (?)

## `insert_rows`

CONSIDER removing in favor `mark_rows_processed_success`/`mark_rows_processed_error`

## `mark_rows_processed_success`/`mark_rows_processed_error`

Set state of given transform tasks

## `mark_all_rows_unprocessed`

Force recomputation of the step

TODO move to TransformMetaTable

## DONE compute_transform_schema

## TODO get_changed_idx_count

## TODO get_full_process_ids

## TODO get_change_list_process_ids

## fill_metadata/reset_metadata

## run_full

CONSIDER add `start_run_full` / `complete_run`

## run_changelist

CONSIDER add `start_run_changelist` / `complete_run`