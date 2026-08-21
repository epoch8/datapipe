# Tables and TableStores

A Datapipe **table** is a named dataset with a primary key schema. The **TableStore** is the backend that holds the actual rows (SQL, files, Redis, vectors, …). Metadata for incremental processing lives separately in the SQL meta plane — not inside your business table.

## Catalog and Table

```python
from datapipe.compute import Catalog, Table
from datapipe.store.database import TableStoreDB

catalog = Catalog({
    "words": Table(store=TableStoreDB(dbconn, Word.__table__)),
    "word_lengths": Table(store=TableStoreDB(dbconn, WordLength.__table__)),
})
```

- **`Catalog`** — name → `Table` map used by the pipeline.
- **`Table`** — thin wrapper around a `TableStore`.
- **`DataStore`** — runtime handle: opens meta plane, creates `DataTable` instances, runs steps.

At build time (`build_compute` / app startup), every catalog table is initialized so meta tables exist.

## Data vs meta

| Layer | What it stores | Example |
|---|---|---|
| TableStore (data) | Your columns | `word_id`, `text` |
| `{name}_meta` | Hash + timestamps | `hash`, `update_ts`, `delete_ts` |
| `{step}_meta` | Per-key transform state | `process_ts`, `is_success` |

Reading live rows always filters `delete_ts IS NULL`. Soft-deleted keys remain in meta so deletes can still schedule downstream work. See [Incremental Processing](./incremental-processing.md).

## Choosing a store

| Backend | Good for | Extra |
|---|---|---|
| [Database](../reference/stores/database.md) (`TableStoreDB`) | Structured rows, Postgres/SQLite | — |
| [Filedir](../reference/stores/filedir.md) | Images, JSON, Parquet on disk/S3 | `s3fs` / `gcsfs` / `pyarrow` as needed |
| [Redis](../reference/stores/redis.md) | Fast KV-ish tables | `redis` |
| [Elasticsearch](../reference/stores/elastic.md) | Search documents | `elastic` |
| [Qdrant](../reference/stores/qdrant.md) / [Milvus](../reference/stores/milvus.md) | Vectors | `qdrant` / `milvus` |
| Neo4j / Excel / JSON lines | Niche; see [stores index](../reference/stores/index.md) | `neo4j` / `excel` |

Implement a custom backend with [Write a Custom TableStore](../how-to/custom-table-store.md).

## External tables

Some tables are filled outside Datapipe (files dropped on disk, rows inserted by another service). Use [`UpdateExternalTable`](../reference/steps/update-external-table.md) so meta hashes catch up and downstream steps see inserts/updates/deletes. How-to: [Pull Data from External Sources](../how-to/external-sources.md).

## See also

- [Table](../reference/table.md) — `Table` / `DataTable` / `DataStore` reference
- [Primary Keys and Transform Keys](./primary-keys.md)
