# Milvus store

When to use: Vector embeddings in a Milvus collection (ANN search companion to a SQL/file table of metadata).

Module: `datapipe.store.milvus`  
Extra: `datapipe-core[milvus]`

```python
from pymilvus import DataType, FieldSchema
from sqlalchemy import Column, Integer

from datapipe.store.milvus import MilvusStore

store = MilvusStore(
    name="image_embeddings",
    schema=[
        FieldSchema(name="image_id", dtype=DataType.INT64, is_primary=True),
        FieldSchema(name="embedding", dtype=DataType.FLOAT_VECTOR, dim=512),
    ],
    primary_db_schema=[Column("image_id", Integer, primary_key=True)],
    index_params={"index_type": "IVF_FLAT", "metric_type": "L2", "params": {"nlist": 128}},
    pk_field="image_id",
    embedding_field="embedding",
    connection_details={"host": "localhost", "port": "19530"},
)
```

Typical pattern: keep image metadata in `TableStoreDB` / Filedir; store vectors here; join in a `BatchTransform` by `image_id`.

### Arguments

| Arg | Description |
|---|---|
| `name` | Collection name |
| `schema` | Milvus `FieldSchema` list |
| `primary_db_schema` | Datapipe PK columns (SQLAlchemy) |
| `index_params` | Vector index config |
| `pk_field` / `embedding_field` | Field names inside the collection |
| `connection_details` | Passed to `pymilvus.connections.connect` |

### Notes

- Inserts release a loaded collection so the next search reloads.
- Not a general-purpose row store — pair with a metadata table for non-vector columns.

### See also

- [Stores index](./index.md)
- [Qdrant](./qdrant.md) — alternative vector backend
- FiftyOne / embeddings examples under Integrations
