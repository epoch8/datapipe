# DatapipeApp and API

Clarify core vs Ops app types, how `datapipe api` starts the server, and which env vars matter.

## Two “DatapipeApp” names

| Symbol | Import | Role |
|---|---|---|
| Core app | `from datapipe.compute import DatapipeApp` | Holds `ds`, `catalog`, `pipeline`, builds compute steps. Enough for CLI `run` / `step` / `table` / `db` without HTTP Ops. |
| Re-export | `from datapipe_app import DatapipeApp` | **Same class** — lazy re-export of `datapipe.compute.DatapipeApp` for convenience. |
| Ops HTTP app | `from datapipe_app import DatapipeAPI` | FastAPI subclass that **is-a** `DatapipeApp` plus routes, static UI, observability, ops-specs. |

Use `DatapipeAPI` when you want the dashboard and run history. Core-only scripts can keep `datapipe.compute.DatapipeApp`.

## Construction

Core / re-export:

```python
from datapipe_app import DatapipeApp  # or datapipe.compute.DatapipeApp

app = DatapipeApp(ds, catalog, pipeline)
```

Ops API:

```python
from datapipe_app import DatapipeAPI

app = DatapipeAPI(ds, catalog, pipeline)
# optional:
#   pipeline_id=...,
#   create_observability_tables=...,
#   run_logs_backend=...,
#   observability_dbconn=...,
```

| Arg | Type | Role |
|---|---|---|
| `ds` | `DataStore` | Meta plane + table access |
| `catalog` | `Catalog` | Named tables |
| `pipeline` | `Pipeline` | Step graph |

At init, compute steps are built (`build_compute`) the same way as core. You can also wrap an existing core `DatapipeApp` via `DatapipeAPI(app=core_app)` — but not wrap an existing `DatapipeAPI` again.

## CLI: `datapipe api`

`datapipe-app` registers the `api` subcommand (`datapipe_app.app.cli:register_commands`). It wraps the loaded pipeline in `DatapipeAPI` if needed, then serves with **uvicorn**:

```bash
datapipe --pipeline app:app api
datapipe --pipeline app:app api --host 127.0.0.1 --port 8000
```

| Option | Default | Role |
|---|---|---|
| `--host` | `0.0.0.0` | Bind address |
| `--port` | `8000` | Port |

If `--pipeline` already points at a `DatapipeAPI` instance, that object is used as-is (executor from `--executor` is applied when set).

Entry-point registration is described in [Extend the CLI](../how-to/extend-cli.md). General CLI flags (`--pipeline`, `--executor`, …) are in [Datapipe CLI](../reference/cli.md).

## OpenAPI /docs

`DatapipeAPI` is a FastAPI app. When the server is up, interactive OpenAPI is available at the usual FastAPI paths:

- `/docs` — Swagger UI
- `/redoc` — ReDoc
- `/openapi.json` — schema

## HTTP surface (overview)

Exact routes evolve with the app version; typical groups:

| Area | Path prefix | Purpose |
|---|---|---|
| Ops UI | `/` | SPA |
| Debug graph | `/graph` | Graph visualization |
| Runs | `/api/v1alpha3/runs` | List / inspect runs |
| Pipelines | `/api/v1alpha3/pipelines/{id}` | Pipeline metadata |
| Ops specs | `/ops-specs/*` | Spec-driven ops panels |
| Metrics | `/pipelines/{id}/metrics/*` | Catalog / ML metrics when enabled |

ML extensions come from `datapipe-app-ml-ops` when the `[ml]` extra is installed.

## Environment

| Variable | Role |
|---|---|
| `DATAPIPE_APP_PIPELINE_ID` | Default pipeline id for single-pipeline apps |
| `DATAPIPE_APP_SHOW_STEP_STATUS` | Include step dirty/status in API responses (default `false`) |
| `DATAPIPE_APP_RECORD_CLI_RUNS` | Record CLI runs in observability (default `true`) |
| `DATAPIPE_APP_CREATE_OBSERVABILITY_TABLES` | Auto-create observability tables on API start (default `false`) |
| `DB_CONN_URI` / example `DB_URL` | Metadata and ops storage (app-specific; see example READMEs) |

Full observability detail: [Observability](./observability.md).

## See also

- [Install and run Ops](./install-and-run.md)
- [Ops UI walkthrough](./ui-walkthrough.md)
- Core [Pipeline / Catalog / DatapipeApp](../reference/pipeline-catalog.md)
