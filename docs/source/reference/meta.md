# TableMeta and TransformMeta

Metadata interfaces for row-level change tracking (table meta) and transform-grain processing state (transform meta).

Modules: `datapipe.meta.base` (abstract API), `datapipe.meta.sql_meta` (default SQL implementation via `SQLMetaPlane`).

Factory: `DataStore.meta_plane` (`MetaPlane.create_table_meta`, `MetaPlane.create_transform_meta`).

---

## `TableDebugInfo`

When to use: Lightweight debug snapshot from `TableMeta.get_table_debug_info`.

```python
@dataclass
class TableDebugInfo:
    name: str
    size: int
```

| Field | Type | Description |
|---|---|---|
| `name` | `str` | Table name. |
| `size` | `int` | Metadata row count (typically non-deleted rows). |

---

## `TableMeta`

When to use: Per-table metadata backing a `DataTable` — hashes, timestamps, soft deletes. Default implementation: `SQLTableMeta` (`{name}_meta` SQL table).

### Overview

| Member | Type | Description |
|---|---|---|
| `primary_schema` | `DataSchema` | Primary-key columns. |
| `primary_keys` | `list[str]` | Primary-key column names. |

---

## `TableMeta.get_metadata`

When to use: Load metadata rows, optionally filtered by index.

```python
def get_metadata(
    self,
    idx: IndexDF | None = None,
    include_deleted: bool = False,
) -> MetadataDF: ...
```

### Parameters

| Name | Type | Default | Description |
|---|---|---|---|
| `idx` | `IndexDF \| None` | `None` | Limit to these primary keys; `None` = all rows. |
| `include_deleted` | `bool` | `False` | Include rows with `delete_ts` set. |

### Returns

`MetadataDF` — index columns plus `hash`, `create_ts`, `update_ts`, `process_ts`, `delete_ts`, and store-specific meta columns.

### Behavior notes

- SQL backend chunks large `idx` filters (~5000 / num_primary_keys rows per query).

---

## `TableMeta.get_metadata_size`

When to use: Count metadata rows without loading them.

```python
def get_metadata_size(
    self,
    idx: IndexDF | None = None,
    include_deleted: bool = False,
) -> int: ...
```

### Parameters

Same as `get_metadata`.

### Returns

`int` — row count.

---

## `TableMeta.get_existing_idx`

When to use: Resolve which indexes exist in meta (not deleted). Used by `DataTable.get_data`.

```python
def get_existing_idx(self, idx: IndexDF | None = None) -> IndexDF: ...
```

### Parameters

| Name | Type | Default | Description |
|---|---|---|---|
| `idx` | `IndexDF \| None` | `None` | If set, return intersection with existing rows; empty index → empty result. |

### Returns

`IndexDF` — primary-key columns only; excludes deleted rows.

### Behavior notes

- Filters `idx` to columns that overlap table primary keys before querying.

---

## `TableMeta.get_table_debug_info`

```python
def get_table_debug_info(self) -> TableDebugInfo: ...
```

### Returns

`TableDebugInfo`

---

## `TableMeta.get_changes_for_store_chunk`

When to use: Compare incoming row hashes with stored meta to classify inserts, updates, and reactivations. Called from `DataTable.store_chunk`.

```python
def get_changes_for_store_chunk(
    self,
    hash_df: HashDF,
    now: float | None = None,
) -> tuple[IndexDF, IndexDF, MetadataDF, MetadataDF]: ...
```

### Parameters

| Name | Type | Default | Description |
|---|---|---|---|
| `hash_df` | `HashDF` | — | Index + `hash` from `table_store.hash_rows`. |
| `now` | `float \| None` | `None` | Timestamp for new/updated meta; defaults to `time.time()`. |

### Returns

`tuple[IndexDF, IndexDF, MetadataDF, MetadataDF]`:

| Element | Meaning |
|---|---|
| `new_index_df` | Keys to insert |
| `changed_index_df` | Keys whose hash changed |
| `new_meta_df` | Meta rows for inserts |
| `changed_meta_df` | Meta rows for updates |

### Behavior notes

- Treats missing meta or non-null `delete_ts` as new.
- Changed rows reset `delete_ts` and bump `update_ts` / `process_ts`.

---

## `TableMeta.update_rows`

When to use: Upsert metadata rows after data writes.

```python
def update_rows(self, df: MetadataDF) -> None: ...
```

### Parameters

| Name | Type | Description |
|---|---|---|
| `df` | `MetadataDF` | Rows to upsert on primary key. |

### Returns

`None`

### Behavior notes

- No-op when `df` is empty.
- SQL: `INSERT … ON CONFLICT DO UPDATE` on hash and timestamp fields.

---

## `TableMeta.mark_rows_deleted`

When to use: Soft-delete metadata (sets `delete_ts`, zeros hash). Called from `DataTable.delete_by_idx`.

```python
def mark_rows_deleted(
    self,
    deleted_idx: IndexDF,
    now: float | None = None,
) -> None: ...
```

### Parameters

| Name | Type | Default | Description |
|---|---|---|---|
| `deleted_idx` | `IndexDF` | — | Rows to mark deleted. |
| `now` | `float \| None` | `None` | Deletion timestamp; defaults to `time.time()`. |

### Returns

`None`

### Behavior notes

- No-op when `deleted_idx` is empty.

---

## `TableMeta.get_stale_idx`

When to use: Iterate indexes with `process_ts` below a watermark. Used by generators and `delete_stale_by_process_ts`.

```python
def get_stale_idx(
    self,
    process_ts: float,
    run_config: RunConfig | None = None,
) -> Generator[IndexDF, None, None]: ...
```

### Parameters

| Name | Type | Description |
|---|---|---|
| `process_ts` | `float` | Rows with meta `process_ts < process_ts` and not deleted are stale. |
| `run_config` | `RunConfig \| None` | Optional label filters applied in SQL backend. |

### Returns

Generator yielding `IndexDF` chunks (1000 rows in SQL backend).

---

## `TableMeta.reset_metadata`

When to use: Force full reprocessing by resetting service timestamps on all (or filtered) rows.

```python
def reset_metadata(self, run_config: RunConfig | None = None) -> None: ...
```

### Parameters

| Name | Type | Description |
|---|---|---|
| `run_config` | `RunConfig \| None` | May limit rows via filters (implementation-dependent). |

### Returns

`None`

---

## `TableMeta.transform_idx_to_table_idx`

When to use: Map transform-grain index columns to table primary-key columns (inverse of `InputSpec.keys` / `OutputSpec.keys`).

```python
def transform_idx_to_table_idx(
    self,
    transform_idx: IndexDF,
    keys: dict[str, str] | None = None,
) -> IndexDF: ...
```

### Parameters

| Name | Type | Default | Description |
|---|---|---|---|
| `transform_idx` | `IndexDF` | — | Index with transform key column names. |
| `keys` | `dict[str, str] \| None` | `None` | Map `{transform_key: table_key}`; `None` returns `transform_idx` unchanged. |

### Returns

`IndexDF` — columns renamed/mapped to table keys.

### Example

```python
table_idx = dt.meta.transform_idx_to_table_idx(batch_idx, {"item_id": "id"})
data = dt.get_data(table_idx)
```

---

## `TransformMeta`

When to use: Per-transform metadata at transform-key grain — tracks `process_ts`, success, errors, priority. Default: `SQLTransformMeta` (table named `{step_name}_meta`).

### Overview

| Member | Type | Description |
|---|---|---|
| `transform_keys_schema` | `DataSchema` | SQL columns for transform keys. |
| `transform_keys` | `list[str]` | Transform grain column names. |

---

## `TransformMeta.compute_transform_schema`

When to use: Infer or validate transform keys from input/output `ComputeInput` / `ComputeOutput` bindings.

```python
@classmethod
def compute_transform_schema(
    cls,
    inputs: Sequence[ComputeInput],
    outputs: Sequence[ComputeOutput],
    transform_keys: list[str] | None,
) -> tuple[list[str], MetaSchema]: ...
```

### Parameters

| Name | Type | Description |
|---|---|---|
| `inputs` | `Sequence[ComputeInput]` | Step inputs (with key maps). |
| `outputs` | `Sequence[ComputeOutput]` | Step outputs. |
| `transform_keys` | `list[str] \| None` | Explicit grain; `None` = infer intersection of input primary keys with output primary keys (or input-only if no outputs). |

### Returns

`tuple[list[str], MetaSchema]` — key names and SQLAlchemy column list.

### Raises

| Exception | When |
|---|---|
| `AssertionError` | No inputs; empty inferred intersections. |

---

## `TransformMeta.get_changed_idx_count`

When to use: Count indexes needing processing (inputs newer than last success, or never processed / failed).

```python
def get_changed_idx_count(
    self,
    ds: DataStore,
    run_config: RunConfig | None = None,
) -> int: ...
```

### Returns

`int`

### Behavior notes

- SQL backend joins aggregated input `update_ts` against transform meta `process_ts` / `is_success`.

---

## `TransformMeta.get_full_process_ids`

When to use: Chunk iterator for a full transform run.

```python
def get_full_process_ids(
    self,
    ds: DataStore,
    chunk_size: int,
    run_config: RunConfig | None = None,
) -> tuple[int, Generator[IndexDF, None, None]]: ...
```

### Parameters

| Name | Type | Description |
|---|---|---|
| `ds` | `DataStore` | Active datastore. |
| `chunk_size` | `int` | Rows per yielded batch. |
| `run_config` | `RunConfig \| None` | Filters; extra filter keys not in transform keys are injected into yielded indexes. |

### Returns

`tuple[int, Generator[IndexDF, None, None]]` — `(ceil(changed_count / chunk_size), generator)`.

### Behavior notes

- Returns `(0, empty generator)` when there are no inputs.

---

## `TransformMeta.get_change_list_process_ids`

When to use: Chunk iterator scoped to upstream `ChangeList` entries.

```python
def get_change_list_process_ids(
    self,
    ds: DataStore,
    change_list: ChangeList,
    chunk_size: int,
    run_config: RunConfig | None = None,
) -> tuple[int, Generator[IndexDF, None, None]]: ...
```

### Parameters

| Name | Type | Description |
|---|---|---|
| `change_list` | `ChangeList` | Changed indexes per input table name. |
| `chunk_size` | `int` | Rows per batch. |

### Returns

`tuple[int, Generator[IndexDF, None, None]]`

### Behavior notes

- Unions transform keys from each changed input; deduplicates.
- When changelist index lacks transform columns, re-queries via changed-index SQL.

---

## `TransformMeta.insert_rows`

When to use: Pre-create transform-meta rows (`fill_metadata`).

```python
def insert_rows(self, idx: IndexDF) -> None: ...
```

### Parameters

| Name | Type | Description |
|---|---|---|
| `idx` | `IndexDF` | Transform keys to insert. |

### Returns

`None`

### Behavior notes

- SQL: `ON CONFLICT DO NOTHING`; initial `process_ts=0`, `is_success=False`.

---

## `TransformMeta.mark_rows_processed_success`

When to use: Mark a batch successfully processed after outputs are stored.

```python
def mark_rows_processed_success(
    self,
    idx: IndexDF,
    process_ts: float,
    run_config: RunConfig | None = None,
) -> None: ...
```

### Parameters

| Name | Type | Description |
|---|---|---|
| `idx` | `IndexDF` | Transform keys processed. |
| `process_ts` | `float` | Success timestamp. |

### Returns

`None`

### Behavior notes

- No-op when deduplicated index is empty.
- Special case: empty-column index with one row replaces entire transform table (global transform).

---

## `TransformMeta.mark_rows_processed_error`

When to use: Record batch failure without stopping the run (unless `fail_fast`).

```python
def mark_rows_processed_error(
    self,
    idx: IndexDF,
    process_ts: float,
    error: str,
    run_config: RunConfig | None = None,
) -> None: ...
```

### Parameters

| Name | Type | Description |
|---|---|---|
| `error` | `str` | Error message stored in meta. |

### Returns

`None`

---

## `TransformMeta.get_metadata_size`

```python
def get_metadata_size(self) -> int: ...
```

### Returns

`int` — total transform-meta row count.

---

## `TransformMeta.mark_all_rows_unprocessed`

When to use: Reset success state so all rows re-queue (`reset_metadata` on batch steps).

```python
def mark_all_rows_unprocessed(
    self,
    run_config: RunConfig | None = None,
) -> None: ...
```

### Returns

`None`

### Behavior notes

- SQL: sets `process_ts=0`, `is_success=False`, `error=None` for successful rows; honors `run_config` filters.

---

## `MetaPlane`

When to use: Factory on `DataStore` for backend-specific meta objects.

```python
class MetaPlane:
    def create_table_meta(
        self,
        name: str,
        primary_schema: DataSchema,
        meta_schema: MetaSchema,
    ) -> TableMeta: ...

    def create_transform_meta(
        self,
        name: str,
        input_dts: Sequence[ComputeInput],
        output_dts: Sequence[ComputeOutput],
        transform_keys: list[str] | None = None,
        order_by: list[str] | None = None,
        order: Literal["asc", "desc"] = "asc",
    ) -> TransformMeta: ...
```

Default implementation: `SQLMetaPlane` — creates `{name}_meta` table meta and transform meta tables when `create_meta_table=True` on `DataStore`.

---

## See also

- [Table / DataTable / DataStore](./table.md)
- [Meta-table schema](../explanation/meta-table-schema.md)
- [Change detection](../explanation/change-detection.md)
- [BaseBatchTransformStep](./steps/batch-transform.md#basebatchtransformstep)
