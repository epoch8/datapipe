# TableStore backends

Abstract store API and concrete backends in `datapipe.store`.

---

## `TableStore` / `TableStoreCaps`

When to use: Implement or pick a backend that stores row data; meta lives in `DataStore`.

```python
@dataclass
class TableStoreCaps:
    supports_delete: bool
    supports_get_schema: bool
    supports_read_all_rows: bool
    supports_read_nonexistent_rows: bool
    supports_read_meta_pseudo_df: bool

class TableStore(ABC):
    caps: TableStoreCaps
    def get_primary_schema(self) -> DataSchema: ...
    def get_meta_schema(self) -> MetaSchema: ...
    def get_schema(self) -> DataSchema: ...
    def hash_rows(self, df: DataDF) -> HashDF: ...
    def insert_rows(self, df: DataDF) -> None: ...
    def update_rows(self, df: DataDF) -> None: ...  # default: delete + insert
    def delete_rows(self, idx: IndexDF) -> None: ...
    def read_rows(self, idx: IndexDF | None = None) -> DataDF: ...
    def read_rows_meta_pseudo_df(
        self, chunksize: int = 1000, run_config: RunConfig | None = None
    ) -> Iterator[DataDF]: ...
```

### Notes

- Default `hash_rows` hashes `primary_keys + meta_keys` via CityHash32.
- Default `read_rows_meta_pseudo_df` yields a single full `read_rows()` — override for large tables / external sync.

### See also

- [Custom TableStore how-to](../../how-to/custom-table-store.md)

---

## Backend index

| Backend | Module | Doc |
|---|---|---|
| `TableStoreDB` | `datapipe.store.database` | [Database](./database.md) |
| `TableStoreFiledir` + adapters | `datapipe.store.filedir` | [Filedir](./filedir.md) |
| `TableStoreExcel` | `datapipe.store.pandas` | below |
| `TableStoreJsonLine` | `datapipe.store.pandas` | below |
| `RedisStore` | `datapipe.store.redis` | [Redis](./redis.md) |
| `ElasticStore` | `datapipe.store.elastic` | [Elasticsearch](./elastic.md) |
| `QdrantStore` / `QdrantShardedStore` | `datapipe.store.qdrant` | [Qdrant](./qdrant.md) |
| `MilvusStore` | `datapipe.store.milvus` | [Milvus](./milvus.md) |
| `Neo4JStore` | `datapipe.store.neo4j` | below |

---

## `TableStoreExcel`

When to use: Single Excel workbook as a table (local or fsspec path).

```python
class TableStoreExcel(TableDataSingleFileStore):
    def __init__(
        self,
        filename: Path | str | None = None,
        primary_schema: DataSchema | None = None,  # default: id String PK
    ): ...
```

### Notes

- Uses `openpyxl` via pandas; datetime columns coerced from schema dtypes.
- Full-file rewrite on insert/delete (single-file store). Caps: delete yes, get_schema no, read all yes.

---

## `TableStoreJsonLine`

When to use: Single JSON Lines file as a table.

```python
class TableStoreJsonLine(TableDataSingleFileStore):
    def __init__(
        self,
        filename: Path | str | None = None,
        primary_schema: DataSchema | None = None,
    ): ...
```

### Notes

- `orient="records", lines=True`; ISO date format on write.
- Same single-file rewrite semantics as Excel.

---

## `Neo4JStore`

When to use: Push nodes/edges to Neo4j-compatible graph DBs (tested on Memgraph).

```python
class Neo4JStore(TableStore):
    def __init__(
        self,
        connection_kwargs: dict[str, Any],  # passed to GraphDatabase.driver(**)
        data_sql_schema: list[Column],
    ) -> None: ...
```

### Modes (from primary keys)

| Mode | Required PKs |
|---|---|
| Node | `node_id`, `node_type` |
| Edge | `from_node_id`, `to_node_id`, `from_node_type`, `to_node_type`, `edge_label` |

Non-PK columns typically include `attributes` (dict of properties).

### Caps

Delete yes; get_schema yes; read_all / nonexistent / meta_pseudo_df **no** — not suitable as `UpdateExternalTable` source without custom support.

### Notes

- MERGE-based upserts; node deletes use `DETACH DELETE`.
- Two catalog tables (nodes + edges) are the usual pattern.

### See also

- [Tables and TableStores](../../concepts/tables-and-stores.md)
