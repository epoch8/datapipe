# Database store

SQL table backend and metadata connection.

Module: `datapipe.store.database`

---

## `DBConn`

When to use: Share a SQLAlchemy engine + metadata for meta DB and/or `TableStoreDB` data tables.

```python
class DBConn:
    def __init__(
        self,
        connstr: str,
        schema: str | None = None,
        create_engine_kwargs: dict[str, Any] | None = None,
        sqla_metadata: MetaData | None = None,
    ): ...
```

### Arguments

| Arg | Description |
|---|---|
| `connstr` | SQLAlchemy URL. SQLite (`sqlite` / `pysqlite`) vs others (Postgres-oriented). |
| `schema` | Optional DB schema; used for `MetaData` and `ensure_db_schema`. |
| `create_engine_kwargs` | Extra `create_engine` kwargs. |
| `sqla_metadata` | Shared `MetaData`; created if omitted. |

### Notes

- SQLite: `SingletonThreadPool`, `PRAGMA journal_mode=WAL`, `insert` from SQLite dialect, `func.max` as `func_greatest`.
- Else: `QueuePool` with pre-ping/recycle, Postgres `insert`, `func.greatest`, `supports_update_from=True`.
- Pickle-friendly via `__reduce__` / `__getstate__` (recreates engine).

### Helpers

- `ensure_db_schema(dbconn)` — `CREATE SCHEMA IF NOT EXISTS` (no-op for SQLite / empty schema).

---

## `TableStoreDB`

When to use: Persist table data in SQL (same or different DB as meta).

```python
class TableStoreDB(TableStore):
    def __init__(
        self,
        dbconn: DBConn | str,
        name: str | None = None,
        data_sql_schema: list[Column] | None = None,
        create_table: bool = False,
        orm_table: OrmTable | None = None,
    ) -> None: ...
```

### Arguments

| Arg | Description |
|---|---|
| `dbconn` | `DBConn` or connection string. |
| `name` | Table name (required unless `orm_table`). |
| `data_sql_schema` | SQLAlchemy `Column` list with PKs (required unless `orm_table`). |
| `create_table` | If `True`, `CREATE TABLE IF NOT EXISTS` on init. |
| `orm_table` | Declarative ORM class; mutually exclusive with `name` / `data_sql_schema`. |

### Caps

Delete, get_schema, read_all, read_nonexistent, meta_pseudo_df — all **yes**.

### Notes

- Upserts via `ON CONFLICT DO UPDATE` (or `DO NOTHING` when there are no non-PK columns).
- Index filters are chunked (~`5000 // len(primary_keys)`) for Postgres stack limits.
- `read_rows_meta_pseudo_df` streams with `chunksize` and applies `RunConfig` filters.
- Columns marked with `MetaKey()` participate in `get_meta_schema` / hashing extras.

### `MetaKey`

```python
class MetaKey(SchemaItem):
    def __init__(self, target_name: str | None = None) -> None: ...
```

Attach to a `Column` so it is treated as a meta key (included in hash keys beyond primary keys).

### See also

- [Alembic migrations how-to](../../how-to/alembic-migrations.md)
- [SQLite how-to](../../how-to/using-sqlite.md)
- [CLI `db create-all`](../cli.md)
