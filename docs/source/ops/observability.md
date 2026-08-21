# Observability and run logs

How Datapipe Ops records pipeline executions, where tables come from, and how that relates to incremental dirty keys.

## What gets recorded

With `datapipe-app`, runs can be persisted so the Ops UI can show history, status, and logs.

Typical pieces:

- **Run records** — start/finish, status, which steps ran
- **Progress / step events** — via run callbacks hooked into core `RunConfig`
- **Optional ClickHouse run logs** — high-volume log lines for long jobs

Core still owns incremental meta (`*_meta` tables). Observability is an **ops layer** on top of meta, not a replacement for `update_ts` / `process_ts`.

## OpsSettings (`DATAPIPE_APP_` prefix)

Settings live in `datapipe_app.observability.config.settings.OpsSettings` (`env_prefix="DATAPIPE_APP_"`).

| Field | Env var | Default | Role |
|---|---|---|---|
| `pipeline_id` | `DATAPIPE_APP_PIPELINE_ID` | unset (inferred) | Default pipeline id for single-pipeline apps. If unset, resolved from the pipeline module filename, `--pipeline` spec, or caller module. |
| `show_step_status` | `DATAPIPE_APP_SHOW_STEP_STATUS` | `false` | When true, API/UI may include per-step dirty/status (and related Prometheus metrics). Off by default for cheaper list responses. |
| `record_cli_runs` | `DATAPIPE_APP_RECORD_CLI_RUNS` | `true` | When true, CLI `datapipe run` / `step run` record observability runs. Set `false` (or use `--no-callbacks`) to skip recording. |
| `create_observability_tables` | `DATAPIPE_APP_CREATE_OBSERVABILITY_TABLES` | `false` | When true, `DatapipeAPI` / observability store may DDL-create ops tables on construct. **Default is false** — schema must come from Alembic or `datapipe db create-all`. |

You can also pass `create_observability_tables=True` to `DatapipeAPI(...)` to override the env default for that process.

## Creating observability tables

Three supported paths (prefer the first for real apps):

### 1. Alembic (recommended)

Use migrations shipped with your app (see `examples/datapipe_app/alembic/`):

```bash
uv run alembic upgrade head
```

This keeps pipeline meta, data tables, and observability tables in one migration history.

### 2. Dev-only auto-create

```bash
DATAPIPE_APP_CREATE_OBSERVABILITY_TABLES=true datapipe --pipeline app:app api
```

Fine for throwaway SQLite/local DBs. Do not rely on this in production — it bypasses migration review.

### 3. `datapipe db create-all`

Creates registered SQLAlchemy metadata (including observability tables when the `datapipe-app` `db_create_all` hook is installed). See [CLI — `datapipe db`](../reference/cli.md#datapipe-db).

`examples/datapipe_app` prefers Alembic and warns against mixing `create-all` with a managed migration tree.

## ClickHouse run logs

Optional high-volume run-log backend: `datapipe_app.observability.run_logs` (`RunLogsBackend`, `ClickHouseRunLogStore`, SQL / in-memory alternatives).

There is **no** `DATAPIPE_APP_*` env var for ClickHouse in `OpsSettings`. Examples wire it in app code:

```python
from datapipe_app import DatapipeAPI, RunLogsBackend

app = DatapipeAPI(
    ds,
    catalog,
    pipeline,
    run_logs_backend=RunLogsBackend.clickhouse(CLICKHOUSE_RUN_LOGS_URL),
)
```

`CLICKHOUSE_RUN_LOGS_URL` (e.g. `clickhouse://default:@localhost:8123/default`) appears in `examples/e2e_template/.env.example` and `examples/detection_tags/.env.example`. Table creation defaults to off (`create_table=False`); create via Alembic or pass `create_table=True` / `store.ensure_table()`.

If ClickHouse is not configured, runs still work; log persistence may use SQL / null / in-memory stores depending on how the app constructs `RunLogsBackend`.

## Relating runs to incremental work

An Ops **run** is one execution attempt (CLI or API-triggered). Whether a step did heavy work still depends on dirty keys:

| Situation | What you see |
|---|---|
| Empty dirty set | Run (or step) may finish quickly with little or no `func` work |
| New / changed upstream rows | Dirty keys → step processes those indices; run shows real progress |
| Same inputs, same hashes | Incremental skip — run still recorded if `record_cli_runs` is on |

See [Incremental Processing](../concepts/incremental-processing.md).

## See also

- [Install and run Ops](./install-and-run.md)
- [Alembic migrations](../how-to/alembic-migrations.md)
- Package README: `libs/datapipe-app/README.md`
