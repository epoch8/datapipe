Main idea: separate all metadata management to separate package so, that
inteface between compute/execution-plane and meta would be relatively narrow and
we could create alternative meta-plane implementation

The goal of this change would be to extract all metadata manipulation into a
separate package: `datapipe.meta` and define an interface. Long term goal:
enable alternative metadata management implementation without jumping all over
the code. Next alternative metadata management system: Clickhouse-based

# Interface Surface

The metadata management is split into three main components:
1.  `MetaPlane` - Factory and controller
2.  `TableMeta` - Metadata management for data tables
3.  `TransformMeta` - Metadata management for transformations

## `MetaPlane`

Serves as a factory for creating `TableMeta` and `TransformMeta` instances.

Methods:
- `create_table_meta(name, primary_schema, meta_schema) -> TableMeta`
- `create_transform_meta(name, input_dts, output_dts, transform_keys, ...) -> TransformMeta`

## `TableMeta` interface

Manages metadata for a specific table, including tracking changes, updates, and deletion status.

Core methods:

### Reading Metadata
- `get_metadata(idx, include_deleted) -> MetadataDF`: Retrieve metadata for specific indices.
- `get_metadata_size(idx, include_deleted) -> int`: Count metadata rows.
- `get_existing_idx(idx) -> IndexDF`: Check which indices exist in the metadata.
- `get_table_debug_info() -> TableDebugInfo`: Get debug stats (e.g. size).

### Writing/Updating Metadata
- `get_changes_for_store_chunk(hash_df, now)`: Core logic for incremental updates.
    - Input: `hash_df` (hashes of current data)
    - Returns:
        - `new_index_df`: IDs to insert
        - `changed_index_df`: IDs to update
        - `new_meta_df`: Metadata rows for new items
        - `changed_meta_df`: Metadata rows for updated items
- `update_rows(df)`: Apply updates to metadata rows (upsert).
- `mark_rows_deleted(deleted_idx, now)`: Mark rows as deleted.
- `reset_metadata()`: Reset metadata state (e.g. for full reprocessing).

### Processing
- `get_stale_idx(process_ts, run_config)`: Identify IDs that are stale based on `process_ts`.

## `TransformMeta` interface

Manages the state of transformations (execution status, success/error, priority).

Core methods:

### Execution Planning
- `get_changed_idx_count(ds, run_config) -> int`: Count items needing processing.
- `get_full_process_ids(ds, chunk_size, run_config)`: Get all IDs requiring processing (full run).
- `get_change_list_process_ids(ds, change_list, chunk_size, run_config)`: Get IDs requiring processing based on a change list.

### State Management
- `insert_rows(idx)`: Initialize metadata for new rows (unprocessed state).
- `mark_rows_processed_success(idx, process_ts)`: Mark items as successfully processed.
- `mark_rows_processed_error(idx, process_ts, error)`: Mark items as failed.
- `mark_all_rows_unprocessed()`: Force reprocessing of all items.

### Info
- `get_metadata_size()`: Get total tracked items.

---

# Implementation: SQL Reference

The module `datapipe.meta.sql_meta` provides a SQLAlchemy-based implementation:
- `SQLTableMeta`: Stores metadata in `{table_name}_meta` tables.
    - Validates schemas and creates tables if requested.
    - Implements optimized batch updates and chunked reads.
- `SQLTransformMeta`: Stores transform state (success, error, timestamps).
    - Can compute changed indices using complex joins over input/output metadata tables (`_build_changed_idx_sql`).