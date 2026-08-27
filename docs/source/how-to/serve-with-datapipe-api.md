# How to Serve a Pipeline with DatapipeAPI

Expose a Datapipe pipeline over HTTP with the Ops UI — graph, tables, run history, and OpenAPI docs — using the minimal `examples/datapipe_app/` project.

## Goal

Run a small SQL-backed pipeline locally, apply schema with Alembic, and serve it with `datapipe api`.

## Prerequisites

- Python **3.10+**
- [uv](https://docs.astral.sh/uv/)

No Docker required for the default SQLite setup.

## Example repo

[`examples/datapipe_app/`](https://github.com/epoch8/datapipe/tree/master/examples/datapipe_app)

| File | Role |
|------|------|
| `app.py` | `Catalog`, `Pipeline`, `DatapipeAPI` |
| `alembic/` | Schema migrations (pipeline + meta tables) |
| `pyproject.toml` | uv project with editable `datapipe-app` / `datapipe-core` |

The pipeline aggregates `events` into `user_profile` and `user_lang` via one `BatchTransform`. No ML ops plugin — suitable as a base before adding `datapipe-app-ml-ops`.

---

## Step 1 — Install and migrate

From `examples/datapipe_app/`:

```bash
cd examples/datapipe_app
uv sync
export DB_CONN_URI="${DB_CONN_URI:-sqlite+pysqlite3:///store.sqlite}"
uv run alembic upgrade head
```

**Do not** use `datapipe db create-all` in this example — it bypasses migration history. See [Manage Schema Changes with Alembic](./alembic-migrations.md).

### PostgreSQL (optional)

```bash
export DB_CONN_URI=postgresql://postgres:postgres@localhost:5432/postgres
uv run alembic upgrade head
```

`alembic/env.py` reads the URL from `app.ds.meta_dbconn` (same as `DB_CONN_URI` in `app.py`).

---

## Step 2 — Run the pipeline once

With an empty database this is a no-op on transforms (no events yet):

```bash
uv run datapipe run
```

Insert sample rows into `events` (via SQL or your own loader), then re-run to populate downstream tables.

---

## Step 3 — Start the API server

```bash
uv run datapipe api --port 8000
```

Open **http://localhost:8000** — pipeline graph, table browser, runs.

Interactive API docs:

- `/docs` — Swagger UI
- `/redoc` — ReDoc
- `/openapi.json` — schema

See [DatapipeApp and API](../ops/datapipe-app.md).

### Observability tables

By default, Ops observability tables are **not** auto-created on API start. This example creates them via Alembic. For throwaway local DBs:

```bash
DATAPIPE_APP_CREATE_OBSERVABILITY_TABLES=true uv run datapipe api
```

---

## Step 4 — Docker (optional)

From the repository root:

```bash
docker build -f examples/datapipe_app/Dockerfile -t datapipe-app-example .
docker run --rm -p 8000:8000 datapipe-app-example
```

The image runs `alembic upgrade head` at build time, then `datapipe api` on start.

---

## Verify

```bash
curl -s http://localhost:8000/openapi.json | head -c 200
```

Expect JSON describing FastAPI routes. In the browser at `/`, the pipeline graph should show the `agg_profile` transform with inputs `events` and outputs `user_profile`, `user_lang`.

After loading events and running `uv run datapipe run`:

```bash
uv run datapipe table user_profile list
```

---

## Extend toward ML Ops

To add metrics, training panels, and image views (as in the e2e templates):

1. Replace `DatapipeAPI` construction with `run_logs_backend=RunLogsBackend.clickhouse(...)` if using ClickHouse run logs.
2. Register ops specs via `app.add_specs([DatapipeOpsSpec(...)])` from `datapipe_app_ml_ops`.
3. Build UI assets: `yarn workspace @datapipe/ui build:package`.

See [E2E Image Detection Walkthrough](../getting-started/e2e-image-detection-walkthrough.md) and [Install and run Ops](../ops/install-and-run.md).

---

## See also

- Example README: [`examples/datapipe_app/README.md`](https://github.com/epoch8/datapipe/tree/master/examples/datapipe_app)
- [Extend the CLI](./extend-cli.md)
- [Pipeline / Catalog / DatapipeApp](../reference/pipeline-catalog.md)
