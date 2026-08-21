# Table / DataTable / DataStore

Runtime table handles and the datastore that owns metadata + table instances.

Modules: `datapipe.compute` (`Table`), `datapipe.datatable` (`DataTable`, `DataStore`)

---

## `Table`

When to use: Catalog entry — name a `TableStore` for the pipeline.

```python
@dataclass
class Table:
    store: TableStore
    name: str | None = None
```

Brief description: declarative wrapper; the live object used in transforms is `DataTable`. Full catalog behavior: [Pipeline / Catalog](./pipeline-catalog.md#catalog).

---

## `DataStore`

When to use: Hold the metadata DB connection and create/lookup `DataTable` instances.

```python
class DataStore:
    def __init__(
        self,
        meta_dbconn: DBConn,
        create_meta_table: bool = False,
    ) -> None: ...

    def create_table(self, name: str, table_store: TableStore) -> DataTable: ...
    def get_or_create_table(self, name: str, table_store: TableStore) -> DataTable: ...
    def get_table(self, name: str) -> DataTable: ...
```

### Arguments

| Arg | Description |
|---|---|
| `meta_dbconn` | Connection used for meta / transform-meta tables (`SQLMetaPlane`). |
| `create_meta_table` | If `True`, create meta infrastructure on init. |

### Notes

- `tables` is an in-memory registry; `get_or_create_table` is what `Catalog.get_datatable` uses.
- Owns `event_logger` and `meta_plane`.

### See also

- [DBConn](./stores/database.md#dbconn)

---

## `DataTable`

When to use: Read/write data and metadata for one named table during a run.

```python
class DataTable:
    def __init__(
        self,
        name: str,
        meta: TableMeta,
        table_store: TableStore,
        event_logger: EventLogger,
    ): ...

    # attrs: name, meta, table_store, primary_schema, primary_keys

    def get_metadata(self, idx: IndexDF | None = None) -> MetadataDF: ...
    def get_data(self, idx: IndexDF | None = None) -> DataDF: ...
    def get_size(self) -> int: ...
    def reset_metadata(self) -> None: ...
    def store_chunk(...) -> IndexDF: ...
    def delete_by_idx(...) -> None: ...
    def delete_stale_by_process_ts(...) -> None: ...
```

### Notes

- Primary keys come from the store schema via meta.
- `get_data` only returns rows that exist in meta (`get_existing_idx`).

---

## `store_chunk`

When to use: Upsert a batch of rows and optionally delete indexes that were “processed” but missing from the chunk.

```python
def store_chunk(
    self,
    data_df: DataDF,
    processed_idx: IndexDF | None = None,
    now: float | None = None,
    run_config: RunConfig | None = None,
) -> IndexDF: ...
```

### Arguments

| Arg | Description |
|---|---|
| `data_df` | Rows to insert/update. Index values on primary keys must be unique. |
| `processed_idx` | If set, rows in this index that exist in meta but are absent from `data_df` are deleted. Ignored if it shares no columns with the table’s primary keys. |
| `now` | Timestamp for meta updates; defaults inside meta helpers. |
| `run_config` | Optional run context (logging / filters). |

### Notes

- Compares content hashes via `table_store.hash_rows` to split new vs changed.
- Writes data (`insert_rows` / `update_rows`) then updates meta.
- Returns concatenated change indexes (new + changed + deleted).

---

## `delete_by_idx`

When to use: Soft-delete in meta and remove rows from the store.

```python
def delete_by_idx(
    self,
    idx: IndexDF,
    now: float | None = None,
    run_config: RunConfig | None = None,
) -> None: ...
```

### Notes

- No-op when `idx` is empty.
- Calls `table_store.delete_rows` then `meta.mark_rows_deleted`.

---

## `delete_stale_by_process_ts`

When to use: Remove rows whose meta `process_ts` is older than a watermark (used by generators / external sync).

```python
def delete_stale_by_process_ts(
    self,
    process_ts: float,
    now: float | None = None,
    run_config: RunConfig | None = None,
) -> None: ...
```

### See also

- [Tables and TableStores](../concepts/tables-and-stores.md)
- [Change detection](../explanation/change-detection.md)
- [TableStore](./stores/index.md)
