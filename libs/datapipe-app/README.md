# datapipe-app

FastAPI REST API, `datapipe` CLI, and observability hooks for Datapipe pipelines. The Ops UI is **not** embedded here — it ships in [`datapipe-ui`](../datapipe-ui/) (core) and [`datapipe-ui-ml`](../datapipe-ui-ml/) (ML plugin SPA). ML API extensions live in [`datapipe-app-ml-ops`](../datapipe-app-ml-ops/).

## Install

```bash
# API only
uv pip install -e "libs/datapipe-app"

# API + Ops UI
uv pip install -e "libs/datapipe-app[ui]"

# API + UI + ML ops backend + ML UI plugin
uv pip install -e "libs/datapipe-app[ml]"
```

Build static assets before running the API locally:

```bash
make -C libs/datapipe-ui build-package      # core-only UI
make -C libs/datapipe-ui-ml build-package   # ML UI (preferred when [ml] extra is installed)
```

## Usage

```python
from datapipe_app import DatapipeApp

from pipeline import ds, catalog, pipeline

app = DatapipeApp(ds, catalog, pipeline)
```

`DatapipeAPI` adds the Ops dashboard (`/`) and debug graph UI (`/graph`). Start with:

```bash
datapipe --pipeline app:app api
```

Ops API: `/api/v1alpha3/runs`, `/pipelines/{id}`, spec-driven `/ops-specs/*`, catalog metrics under `/pipelines/{id}/metrics/*`.

Optional env: `DATAPIPE_APP_PIPELINE_ID` (default pipeline id for single-pipeline apps).

## Makefile

From `libs/datapipe-app`:

| Target | Description |
|--------|-------------|
| `make build-ui` | Build frontend + `datapipe-ui` Python wheel |
| `make build-frontend` | Same via Docker (Node 18) |
| `make lint` | black, flake8, mypy |
| `make build-example` | Docker image for `examples/datapipe_app` |

## Tests

```bash
uv sync --package datapipe-app --package datapipe-core --extra sqlite
uv run pytest libs/datapipe-app/tests
```

CI: `.github/workflows/lib-datapipe-app.yml`
