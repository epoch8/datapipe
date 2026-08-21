# How to Use SQLite as Metadata Store

Run Datapipe locally against a SQLite file instead of PostgreSQL.

## Goal

Configure `DBConn` with a SQLite URL that supports the SQL Datapipe needs (including `FULL OUTER JOIN`).

## Why not the stdlib sqlite3?

Python’s bundled SQLite is often older than **3.39.0**. Datapipe’s meta queries need a newer engine, so do not use the default `sqlite://` driver with the system library for production of meta state.

## Steps

### 1. Install the `sqlite` extra

```bash
pip install "datapipe-core[sqlite]"
```

Or in `pyproject.toml`:

```toml
dependencies = [
    "datapipe-core[sqlite]",
]
```

That pulls in `pysqlite3-binary` and `sqlalchemy-pysqlite3` with a recent SQLite build. Prefer this over the plain `pysqlite3` package, which can ship an outdated SQLite.

### 2. Open `DBConn` with the `sqlite+pysqlite3` driver

```python
from datapipe.store.database import DBConn
from datapipe.datatable import DataStore

dbconn = DBConn("sqlite+pysqlite3:///db.sqlite")
ds = DataStore(dbconn)
```

Use three slashes for a relative path (`sqlite+pysqlite3:///db.sqlite`) or four for absolute (`sqlite+pysqlite3:////tmp/db.sqlite`).

Helper used by the core examples:

```python
# examples/datapipe_core/_sqlite.py
def sqlite_connstr(path: str = "db.sqlite") -> str:
    return f"sqlite+pysqlite3:///{path}"
```

### 3. Create schema and run

```bash
datapipe db create-all
datapipe run
```

For throwaway local experiments this is enough. For durable schema history, prefer [Alembic migrations](./alembic-migrations.md) instead of relying on `create-all` alone.

## Expected result

- Meta and (when using SQL stores) data tables live in the SQLite file you named.
- Incremental joins that need modern SQLite features work without upgrading the OS package.

## Tip

Keep the SQLite file next to the pipeline for demos; point `DBConn` at PostgreSQL in shared or production environments. The rest of the pipeline code stays the same.

## See also

- [Installation](../getting-started/installation.md)
- [Your First Pipeline](../getting-started/first-pipeline.md)
- [Manage Schema Changes with Alembic](./alembic-migrations.md)
