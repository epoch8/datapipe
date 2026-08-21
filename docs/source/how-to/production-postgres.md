# How to Use PostgreSQL in Production

Point Datapipe meta (and usually SQL data tables) at PostgreSQL via a single `DBConn` URL.

## Goal

Run pipelines against Postgres instead of a local SQLite file, with schema owned by Alembic.

## Steps

### 1. Install

`psycopg` (v3) is a core dependency of `datapipe-core`. No extra is required for the driver.

```bash
pip install datapipe-core
# or from the monorepo: uv sync --package datapipe-core
```

### 2. Open `DBConn` with a Postgres URL

```python
from datapipe.store.database import DBConn
from datapipe.datatable import DataStore

dbconn = DBConn(
    "postgresql+psycopg://user:password@db.example.com:5432/datapipe",
    # schema="pipeline",  # optional Postgres schema
)
ds = DataStore(dbconn)
```

Use the same `DBConn` (or the same URL) for `TableStoreDB` data tables when meta and data should share one database.

### 3. Prefer Alembic over `db create-all`

In shared and production databases, version schema with Alembic:

```bash
alembic upgrade head
```

Do **not** rely on `datapipe db create-all` for prod. That command is for empty local scratch DBs. If the database already has a stamped `alembic_version` table, `create-all` **refuses** to mutate it.

See [Manage Schema Changes with Alembic](./alembic-migrations.md).

## Expected result

- Meta tables (`*_meta`) and SQL data tables live in Postgres.
- Incremental joins use Postgres-oriented SQL (`QueuePool`, `INSERT … ON CONFLICT`, etc.).
- Schema changes ship as Alembic revisions, not ad-hoc `CREATE TABLE`.

## Contrast with SQLite

For local demos, use [SQLite as Metadata Store](./using-sqlite.md) (`sqlite+pysqlite3:///…`). Switch only the connection string (and migration target) for production — pipeline code stays the same.

## See also

- [Alembic migrations](./alembic-migrations.md)
- [Database store reference](../reference/stores/database.md)
- [CLI `db create-all`](../reference/cli.md)
