# Table / DataTable / DataStore

Runtime table handles and the datastore that owns metadata, table instances, and the meta factory.

Modules: `datapipe.compute` (`Table`), `datapipe.datatable` (`DataTable`, `DataStore`)

---

## `Table`

When to use: Catalog entry — associate a name with a `TableStore` for declarative pipeline I/O.

```python
@dataclass
class Table:
    store: TableStore
    name: str | None = None
```

### Fields

| Name | Type | Default | Description |
|---|---|---|---|
| `store` | `TableStore` | — | Backend holding row data. |
| `name` | `str \| None` | `None` | Required when passing inline `Table` to steps instead of a catalog key. |

### Behavior notes

- The live object used during execution is `DataTable`, resolved via `Catalog.get_datatable`.
- Full catalog behavior: [Pipeline / Catalog](./pipeline-catalog.md#catalog).

### See also

- [Tables and TableStores](../concepts/tables-and-stores.md)

---

## `DataStore`

When to use: Root runtime object — metadata DB connection, in-memory table registry, event logging, and `SQLMetaPlane` factory.

### Overview

| Member | Type | Description |
|---|---|---|
| `meta_dbconn` | `DBConn` | SQL connection for meta and transform-meta tables. |
| `event_logger` | `EventLogger` | Step/batch exception and completion logging. |
| `tables` | `dict[str, DataTable]` | In-memory registry of created tables. |
| `meta_plane` | `SQLMetaPlane` | Creates `TableMeta` / `TransformMeta` instances. |

---

## `DataStore.__init__`

```python
def __init__(
    self,
    meta_dbconn: DBConn,
    create_meta_table: bool = False,
) -> None: ...
```

### Parameters

| Name | Type | Default | Description |
|---|---|---|---|
| `meta_dbconn` | `DBConn` | — | Connection for meta infrastructure. |
| `create_meta_table` | `bool` | `False` | When `True`, create meta SQL tables on `TableMeta` / `TransformMeta` construction. |

### Returns

`None`

### Behavior notes

- Initializes `SQLMetaPlane(dbconn=meta_dbconn, create_meta_table=create_meta_table)`.
- `tables` starts empty.

### Example

```python
from datapipe.store.database import DBConn

ds = DataStore(DBConn("sqlite:///meta.db"), create_meta_table=True)
```

---

## `DataStore.create_table`

When to use: Register a new named table and build its `TableMeta`.

```python
def create_table(self, name: str, table_store: TableStore) -> DataTable: ...
```

### Parameters

| Name | Type | Description |
|---|---|---|
| `name` | `str` | Unique table name in this datastore. |
| `table_store` | `TableStore` | Row storage backend. |

### Returns

`DataTable` — new instance, also stored in `ds.tables[name]`.

### Raises

| Exception | When |
|---|---|
| `AssertionError` | `name` already exists in `ds.tables`. |

### Behavior notes

- Reads `primary_schema` and `meta_schema` from `table_store`.
- Creates meta via `meta_plane.create_table_meta`.

---

## `DataStore.get_or_create_table`

When to use: Resolve a catalog table — primary path from `Catalog.get_datatable`.

```python
def get_or_create_table(self, name: str, table_store: TableStore) -> DataTable: ...
```

### Parameters

| Name | Type | Description |
|---|---|---|
| `name` | `str` | Table name. |
| `table_store` | `TableStore` | Store used only when creating. |

### Returns

`DataTable` — existing registry entry or newly created table.

### Behavior notes

- Does not verify that an existing entry uses the same `table_store`.

---

## `DataStore.get_table`

When to use: Lookup a table that must already exist.

```python
def get_table(self, name: str) -> DataTable: ...
```

### Parameters

| Name | Type | Description |
|---|---|---|
| `name` | `str` | Registered table name. |

### Returns

`DataTable`

### Raises

| Exception | When |
|---|---|
| `KeyError` | Name not in `ds.tables`. |

---

## `DataTable`

When to use: Read/write one table's data and metadata during a pipeline run.

### Overview

| Member | Type | Description |
|---|---|---|
| `name` | `str` | Table name. |
| `meta` | `TableMeta` | Row metadata (hash, timestamps). |
| `table_store` | `TableStore` | Physical row storage. |
| `event_logger` | `EventLogger` | Shared logger from datastore. |
| `primary_schema` | `DataSchema` | Alias of `meta.primary_schema`. |
| `primary_keys` | `list[str]` | Alias of `meta.primary_keys`. |

---

## `DataTable.__init__`

```python
def __init__(
    self,
    name: str,
    meta: TableMeta,
    table_store: TableStore,
    event_logger: EventLogger,
) -> None: ...
```

### Parameters

| Name | Type | Description |
|---|---|---|
| `name` | `str` | Table name. |
| `meta` | `TableMeta` | Metadata backend. |
| `table_store` | `TableStore` | Data backend. |
| `event_logger` | `EventLogger` | Event sink. |

### Returns

`None`

### Behavior notes

- Normally constructed by `DataStore.create_table`, not by application code.

---

## `DataTable.get_metadata`

When to use: Load metadata rows for this table.

```python
def get_metadata(self, idx: IndexDF | None = None) -> MetadataDF: ...
```

### Parameters

| Name | Type | Default | Description |
|---|---|---|---|
| `idx` | `IndexDF \| None` | `None` | Limit to these primary keys. |

### Returns

`MetadataDF` — delegates to `meta.get_metadata(idx)` (excludes deleted rows).

---

## `DataTable.get_data`

When to use: Load row data for indexes that exist in meta.

```python
def get_data(self, idx: IndexDF | None = None) -> DataDF: ...
```

### Parameters

| Name | Type | Default | Description |
|---|---|---|---|
| `idx` | `IndexDF \| None` | `None` | Requested keys; only existing non-deleted rows are read. |

### Returns

`DataDF` — from `table_store.read_rows(meta.get_existing_idx(idx))`.

### Behavior notes

- Missing or deleted indexes are silently omitted (not an error).

### Example

```python
rows = images_dt.get_data(batch_idx)
```

---

## `DataTable.get_size`

When to use: Count non-deleted metadata rows.

```python
def get_size(self) -> int: ...
```

### Returns

`int` — `meta.get_metadata_size(idx=None, include_deleted=False)`.

---

## `DataTable.reset_metadata`

When to use: Force reprocessing by resetting meta service fields.

```python
def reset_metadata(self) -> None: ...
```

### Returns

`None`

### Behavior notes

- Delegates to `meta.reset_metadata()` with no `run_config` (full table).

---

## `DataTable.store_chunk`

When to use: Upsert a batch of rows and optionally delete processed indexes missing from the chunk.

```python
def store_chunk(
    self,
    data_df: DataDF,
    processed_idx: IndexDF | None = None,
    now: float | None = None,
    run_config: RunConfig | None = None,
) -> IndexDF: ...
```

### Parameters

| Name | Type | Default | Description |
|---|---|---|---|
| `data_df` | `DataDF` | — | Rows to insert/update. Primary-key values must be unique. |
| `processed_idx` | `IndexDF \| None` | `None` | If set, deletes meta+data rows present in this index but absent from `data_df`. Ignored when it shares no columns with table primary keys. |
| `now` | `float \| None` | `None` | Timestamp for meta updates. |
| `run_config` | `RunConfig \| None` | `None` | Passed to `delete_by_idx` on cleanup path. |

### Returns

`IndexDF` — concatenation of new, changed, and deleted index rows from this call.

### Raises

| Exception | When |
|---|---|
| `ValueError` | Duplicate primary-key values in `data_df`. |

### Behavior notes

1. When `processed_idx` is set, intersect its columns with `primary_keys`; empty intersection → cleanup disabled.
2. Non-empty `data_df`: hash rows, diff via `meta.get_changes_for_store_chunk`, `insert_rows` / `update_rows` on store, then `meta.update_rows`.
3. Cleanup: `existing_idx(processed_idx) − data_idx` → `delete_by_idx`.
4. Empty `data_df` still runs cleanup when `processed_idx` applies.

### Example

```python
changed = out_dt.store_chunk(result_df, processed_idx=batch_idx, now=time.time())
```

---

## `DataTable.delete_by_idx`

When to use: Remove rows from the store and soft-delete in meta.

```python
def delete_by_idx(
    self,
    idx: IndexDF,
    now: float | None = None,
    run_config: RunConfig | None = None,
) -> None: ...
```

### Parameters

| Name | Type | Default | Description |
|---|---|---|---|
| `idx` | `IndexDF` | — | Primary keys to delete. |
| `now` | `float \| None` | `None` | Deletion timestamp for meta. |
| `run_config` | `RunConfig \| None` | `None` | Accepted for API symmetry; not used in current implementation. |

### Returns

`None`

### Behavior notes

- No-op when `len(idx) == 0`.
- Order: `table_store.delete_rows` then `meta.mark_rows_deleted`.

---

## `DataTable.delete_stale_by_process_ts`

When to use: Delete all rows whose meta `process_ts` is below a watermark (generators, external sync).

```python
def delete_stale_by_process_ts(
    self,
    process_ts: float,
    now: float | None = None,
    run_config: RunConfig | None = None,
) -> None: ...
```

### Parameters

| Name | Type | Description |
|---|---|---|
| `process_ts` | `float` | Rows with `process_ts < process_ts` are stale. |
| `now` | `float \| None` | Passed to each `delete_by_idx` call. |
| `run_config` | `RunConfig \| None` | Passed to `meta.get_stale_idx` for label filtering. |

### Returns

`None`

### Behavior notes

- Iterates `meta.get_stale_idx` in chunks; each chunk is converted to index and deleted.

---

## See also

- [TableMeta / TransformMeta](./meta.md)
- [RunConfig](./run-config.md)
- [DBConn](./stores/database.md#dbconn)
- [Change detection](../explanation/change-detection.md)
- [Tables and TableStores](../concepts/tables-and-stores.md)
