# How to Manage Schema Changes with Alembic

Version SQL schemas (pipeline tables and Datapipe meta) with Alembic instead of ad-hoc `CREATE TABLE`.

## Goal

Apply, roll back, and autogenerate migrations so environments stay in sync as models change.

## When to use what

| Approach | Use for |
|---|---|
| `datapipe db create-all` | Local scratch DBs; optional in-place ADD/ALTER when `datapipe-core[alembic]` is installed |
| Alembic revision history | Shared, production, or any DB that must not drift |

If a database already has a stamped `alembic_version` table, `datapipe db create-all` **refuses** to mutate it — use Alembic upgrades instead.

## Steps

### 1. Install Alembic support

```bash
pip install "datapipe-core[alembic]"
# or: uv add --optional alembic / uv sync --extra alembic
```

Projects that already depend on `alembic` directly (see the app example) also work; the extra documents the supported pairing with Datapipe’s schema helpers.

### 2. Point Alembic at pipeline metadata

Configure `alembic.ini` and `alembic/env.py` so `target_metadata` is the same SQLAlchemy `MetaData` your `DBConn` / catalog uses (including Datapipe meta tables).

Minimal pattern (adapted from [`examples/datapipe_app/alembic/env.py`](https://github.com/epoch8/datapipe/tree/master/examples/datapipe_app/alembic/env.py)):

```python
from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Import your DatapipeApp so meta tables are registered on MetaData
from app import app  # noqa: E402

target_metadata = [app.ds.meta_dbconn.sqla_metadata]
config.set_main_option("sqlalchemy.url", app.ds.meta_dbconn.connstr)


def run_migrations_offline() -> None:
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,  # type: ignore[arg-type]
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    connectable = engine_from_config(
        config.get_section(config.config_ini_section),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,  # type: ignore[arg-type]
        )
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
```

If you share one `MetaData` between declarative ORM models and `DBConn(..., sqla_metadata=Base.metadata)`, autogenerate sees both pipeline tables and Datapipe meta.

### 3. Apply migrations

```bash
alembic upgrade head
alembic current
alembic history
```

### 4. Change models, then autogenerate

After editing ORM / `TableStoreDB` schemas registered on that metadata:

```bash
alembic revision --autogenerate -m "describe change"
# review alembic/versions/<new_file>.py
alembic upgrade head
```

Always review autogenerate output before applying.

### 5. Roll back when needed

```bash
alembic downgrade -1
alembic downgrade <revision>
```

## Expected result

- Schema changes are recorded as revision files and applied the same way in every environment.
- Datapipe meta tables and your data tables stay aligned with the code that defines them.
- `create-all` remains a convenience for empty local DBs, not a substitute for migration history.

## Example

Minimal service with SQLite + Alembic: [`examples/datapipe_app/`](https://github.com/epoch8/datapipe/tree/master/examples/datapipe_app) (see `alembic/` and the README’s Alembic section).

## See also

- [Use SQLite as Metadata Store](./using-sqlite.md)
- [Use PostgreSQL in Production](./production-postgres.md)
- [CLI `db create-all`](../reference/cli.md)
