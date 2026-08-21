# Redis store

When to use: Key/value rows in a Redis hash (lookup by primary key; not for full scans / external sync).

Module: `datapipe.store.redis`

```python
class RedisStore(TableStore):
    def __init__(
        self,
        connection: str,
        name: str,
        data_sql_schema: list[Column],
        cluster_mode: bool = False,
        password: str | None = None,
    ) -> None: ...
```

### Arguments

| Arg | Description |
|---|---|
| `connection` | Redis URL (`Redis.from_url`) or, in cluster mode, URL or `host:port,host:port,...`. |
| `name` | Redis hash key (table namespace). |
| `data_sql_schema` | Columns; PKs vs values split for serialization. |
| `cluster_mode` | Use `RedisCluster` (optional `password`). |
| `password` | Cluster auth when using host list form. |

### Minimal example

```python
from sqlalchemy import Column, Integer, String

from datapipe.store.redis import RedisStore

store = RedisStore(
    connection="redis://localhost:6379/0",
    name="session_rows",
    data_sql_schema=[
        Column("session_id", String, primary_key=True),
        Column("payload", String),
    ],
)
```

Always pass an index to reads — full-table scan is unsupported.

### Caps

| Cap | |
|---|---|
| delete | yes |
| get_schema | no |
| read_all_rows | **no** |
| read_nonexistent_rows | no |
| read_meta_pseudo_df | **no** |

### Notes

- Keys/values JSON-serialized; `HSET` / `HMGET` / `HDEL` via pipeline on insert.
- Unsuitable for `UpdateExternalTable` (no meta pseudo-df).

### See also

- [Stores index](./index.md)
