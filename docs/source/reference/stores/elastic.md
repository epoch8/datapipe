# Elasticsearch store

When to use: Document / search rows in an Elasticsearch index (supports full scan → OK with `UpdateExternalTable`).

Module: `datapipe.store.elastic`  
Extra: `datapipe-core[elastic]`

```python
from sqlalchemy import Column, Integer, String

from datapipe.store.elastic import ElasticStore

store = ElasticStore(
    index="products",
    data_sql_schema=[
        Column("product_id", Integer, primary_key=True),
        Column("title", String),
    ],
    es_kwargs={"hosts": ["http://localhost:9200"]},
)
```

### Arguments

| Arg | Description |
|---|---|
| `index` | Elasticsearch index name |
| `data_sql_schema` | SQLAlchemy columns (PKs + values) |
| `es_kwargs` | Passed to `Elasticsearch(**es_kwargs)` |
| `key_name_remapping` | Optional rename map for document fields |
| `mapping` | Optional index mapping applied on first use |

### Caps

| Cap | |
|---|---|
| delete | yes |
| get_schema | yes |
| read_all_rows | yes |
| read_meta_pseudo_df | yes |
| read_nonexistent_rows | no |

### Notes

- Document ids are derived from primary keys (`get_elastic_id`).
- Prefer for searchable catalogs; use `TableStoreDB` when you need relational joins inside Datapipe.

### See also

- [Stores index](./index.md)
- [UpdateExternalTable](../steps/update-external-table.md)
