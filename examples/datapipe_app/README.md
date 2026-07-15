# Datapipe App example (SQLite + Alembic)

Minimal pipeline with `datapipe-app` only: three SQL tables, one batch transform, Ops UI.
No `datapipe-app-ml-ops`.

Pipeline tables and datapipe metadata are managed with **Alembic**, not `datapipe db create-all`.
Ops observability tables are created automatically on the first `datapipe api` / `datapipe run`.

## Setup

From this directory:

```bash
uv sync
export DB_CONN_URI="${DB_CONN_URI:-sqlite+pysqlite3:///store.sqlite}"
uv run alembic upgrade head
```

## Run

```bash
# one pipeline pass (empty DB → no-op on transforms)
uv run datapipe run

# Ops UI + API
uv run datapipe api --port 8000
```

Open http://localhost:8000 — graph, tables, runs (when recorded).

## Database URL

Default: `sqlite+pysqlite3:///store.sqlite` in the example directory.

PostgreSQL:

```bash
export DB_CONN_URI=postgresql://postgres:postgres@localhost:5432/postgres
uv run alembic upgrade head
```

`alembic/env.py` reads the URL from `app.ds.meta_dbconn` (same as `DB_CONN_URI` in `app.py`).

## Alembic

Config: `alembic.ini`, scripts in `alembic/versions/`.

### Apply migrations (normal workflow)

```bash
uv run alembic upgrade head      # latest
uv run alembic upgrade +1       # one step forward
uv run alembic current          # show revision
uv run alembic history          # list revisions
```

### Roll back

```bash
uv run alembic downgrade -1           # one step back
uv run alembic downgrade <revision>   # to a specific revision
uv run alembic downgrade base         # empty schema (destructive)
```

### Generate a new migration

After changing `catalog` / `TableStoreDB` schemas or datapipe metadata in `app.py`:

```bash
# empty DB or backup first — autogenerate diffs against live DB
uv run alembic revision --autogenerate -m "describe change"
# review alembic/versions/<new_file>.py, then:
uv run alembic upgrade head
```

`env.py` sets `target_metadata` from `app.ds.meta_dbconn.sqla_metadata`, so autogenerate sees pipeline meta tables.
Data tables use `create_table=False` in `app.py` and are included when their models are registered on that metadata.

**Do not use** `datapipe db create-all` in this example — it bypasses migration history and can drift from `alembic/versions/`.

## Docker

From repository root:

```bash
docker build -f examples/datapipe_app/Dockerfile -t datapipe-app-example .
docker run --rm -p 8000:8000 datapipe-app-example
```

The image runs `alembic upgrade head` at build time, then `datapipe api` on start.

## Layout

| File | Role |
|------|------|
| `app.py` | `Catalog`, `Pipeline`, `DatapipeAPI` |
| `alembic/` | schema migrations |
| `pyproject.toml` | uv project; editable `datapipe-app` / `datapipe-core` |
