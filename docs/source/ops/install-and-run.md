# Install and run Ops

Serve the Ops API + dashboard for a local pipeline.

## Install Python packages

From the monorepo (editable) or your project:

```bash
# API + core Ops UI
uv pip install -e "libs/datapipe-app[ui]"

# Or with ML ops backend + ML UI plugin
uv pip install -e "libs/datapipe-app[ml]"
```

PyPI extras work the same way once packages are published: `datapipe-app[ui]` / `datapipe-app[ml]`.

## Build UI static assets

The Python wheel serves pre-built SPA files. Build once from the **monorepo root**.

### Make (recommended)

```bash
# Core dashboard only
make -C libs/datapipe-ui build-package

# Or core + ML plugin (preferred when using [ml])
make -C libs/datapipe-ui-ml build-package
```

`datapipe api` prefers the `datapipe_ui_ml` static entry point over `datapipe_ui` when both are installed.

### Yarn workspaces

Equivalent yarn flow from the monorepo root:

```bash
yarn install
yarn workspace @datapipe/ui build:package
```

For ML UI packaging, follow `libs/datapipe-ui-ml/README.md` (or `make -C libs/datapipe-ui-ml build-package`, which runs the yarn/workspace build and copies static into the Python package).

Without a successful `build-package`, `/` may 404 or miss assets even if Python deps are installed.

## Minimal app module

```python
from datapipe_app import DatapipeAPI
# Core-only (no HTTP Ops): from datapipe.compute import DatapipeApp

from my_pipeline import ds, catalog, pipeline

app = DatapipeAPI(ds, catalog, pipeline)
```

`DatapipeAPI` adds API routes, static UI mounting, and observability hooks. See [DatapipeApp and API](./datapipe-app.md).

## Database / Alembic

Ops observability tables are **not** auto-created by default. Prefer Alembic (see [Alembic migrations](../how-to/alembic-migrations.md) and `examples/datapipe_app/alembic/`).

```bash
export DB_CONN_URI="${DB_CONN_URI:-sqlite+pysqlite3:///store.sqlite}"
uv run alembic upgrade head
```

Throwaway local shortcut:

```bash
DATAPIPE_APP_CREATE_OBSERVABILITY_TABLES=true uv run datapipe --pipeline app:app api
```

Also: `uv run datapipe db create-all` when your app registers the create-all hooks. Details in [Observability](./observability.md).

## Start the API

```bash
datapipe --pipeline app:app api
# or:
datapipe --pipeline app:app api --host 127.0.0.1 --port 8000
```

Open:

- `/` — Ops UI
- `/graph` — debug graph
- `/docs` — OpenAPI (Swagger)

Default pipeline id can be set with `DATAPIPE_APP_PIPELINE_ID`.

## Expected result

Browser shows the pipeline graph; `datapipe run` (or UI-triggered runs) appear under Runs when recording is enabled.

## Full example

Follow [`examples/datapipe_app/README.md`](../../../examples/datapipe_app/README.md) end-to-end (SQLite + Alembic + UI):

```bash
cd examples/datapipe_app
uv sync
export DB_CONN_URI="${DB_CONN_URI:-sqlite+pysqlite3:///store.sqlite}"
uv run alembic upgrade head
uv run datapipe run
uv run datapipe api --port 8000
```

## See also

- [DatapipeApp and API](./datapipe-app.md)
- [Ops UI walkthrough](./ui-walkthrough.md)
- Package README: `libs/datapipe-app/README.md`
