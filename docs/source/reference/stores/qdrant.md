# Qdrant store

When to use: Store embedding vectors + payload in Qdrant collections.

Module: `datapipe.store.qdrant`

---

## `CollectionParams`

Alias of `qdrant_client.http.models.CreateCollection` — passed through when creating a missing collection.

---

## `QdrantStore`

When to use: Single collection; one primary-key field.

```python
class QdrantStore(TableStore):
    def __init__(
        self,
        name: str,
        url: str,
        schema: DataSchema,
        pk_field: str,
        embedding_field: str,
        collection_params: CollectionParams,
        index_schema: dict | None = None,
        api_key: str | None = None,
        force_vectors_to_ram: bool | None = True,
    ): ...
```

### Arguments

| Arg | Description |
|---|---|
| `name` | Collection name. |
| `url` | Qdrant URL (use port `443` with API key; default Qdrant port is `6333`). |
| `schema` | SQLAlchemy columns describing payload + vector field. Exactly one PK column named `pk_field`. |
| `pk_field` | Primary key column. |
| `embedding_field` | Column holding the vector (excluded from payload). |
| `collection_params` | Create-collection params. |
| `index_schema` | `{field: field_schema}` payload indexes (must be payload fields). |
| `api_key` | Optional Qdrant API key. |
| `force_vectors_to_ram` | If `True`, set `on_disk=False` on vector params when creating. |

### Notes

- Point ids = UUID derived from MD5 of PK string.
- Lazy init on first IO; creates collection on 404.
- `read_rows` requires an index — full scan not supported.
- `update_rows` ≡ `insert_rows` (upsert).
- Extra: `datapipe-core[qdrant]`.

Minimal sketch:

```python
from qdrant_client.http import models as rest
from sqlalchemy import Column, Integer

from datapipe.store.qdrant import CollectionParams, QdrantStore

store = QdrantStore(
    name="emb",
    url="http://localhost:6333",
    schema=[Column("id", Integer, primary_key=True)],  # plus embedding column in real use
    pk_field="id",
    embedding_field="embedding",
    collection_params=CollectionParams(
        vectors=rest.VectorParams(size=128, distance=rest.Distance.COSINE),
    ),
)
```

---

## `QdrantShardedStore`

When to use: Shard across collections named by a pattern of primary-key fields.

```python
class QdrantShardedStore(TableStore):
    def __init__(
        self,
        name_pattern: str,  # e.g. "emb_{tenant_id}"
        url: str,
        schema: DataSchema,
        embedding_field: str,
        collection_params: CollectionParams,
        index_schema: dict | None = None,
        api_key: str | None = None,
    ): ...
```

### Notes

- `{param}` placeholders in `name_pattern` must be primary-key columns.
- Groups rows by those fields; upserts/deletes/reads per collection.
- Point ids from joined PK field/value pairs (MD5 → UUID).
- Same full-read limitation as `QdrantStore`.

### See also

- [Milvus](./milvus.md)
- [Stores index](./index.md)
