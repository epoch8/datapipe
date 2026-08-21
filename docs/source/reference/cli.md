# Datapipe CLI

Command-line entry point: `datapipe` (Click; env prefix `DATAPIPE`).

Loads a `DatapipeApp` and runs / inspects pipeline, steps, tables, and DB helpers.

Module: `datapipe.cli`

---

## Global options

Place **before** the subcommand: `datapipe --debug run`, not `datapipe run --debug`.

| Option | Default | Description |
|---|---|---|
| `--pipeline` | `app` | App locator: `module` (symbol `app`) or `module:symbol`. CWD on `sys.path`. Must be `DatapipeApp`. Runs `datapipe.pipeline_init` entry points after load. |
| `--executor` | `SingleThreadExecutor` | `SingleThreadExecutor` or `RayExecutor` (calls `ray.init()`). |
| `--debug` | off | DEBUG logging for Datapipe. |
| `--debug-sql` | off | Log SQLAlchemy engine at INFO. |
| `--trace-stdout` | off | OpenTelemetry spans to console. |
| `--trace-jaeger` | off | Export to Jaeger thrift collector. |
| `--trace-jaeger-host` | `localhost` | Jaeger host. |
| `--trace-jaeger-port` | `14268` | Jaeger port. |
| `--trace-gcp` | off | Google Cloud Trace (needs `opentelemetry-exporter-cloud-trace`). |

### `--pipeline` examples

```bash
datapipe --pipeline app run
datapipe --pipeline my_project.pipeline:app run
```

---

## `datapipe run`

Run **all** compute steps (`run_full` each) once, or in a loop.

| Option | Default | Description |
|---|---|---|
| `--loop` | off | Repeat until interrupted. |
| `--loop-delay` | `30` | Seconds between loops. |
| `--no-callbacks` | off | Skip `datapipe.run_callbacks` entry points (stdout progress callback still attached). |

Uses the global `--executor`. Always wraps with `StdoutRunCallback` (+ optional entry-point callbacks).

---

## `datapipe step`

Filter steps, then run a subcommand.

| Option | Description |
|---|---|
| `--labels` | `key=value,key2=value2` — step must contain **all** pairs. |
| `--name` | Comma-separated **prefixes**; step name must start with any. |

### `step list`

Print filtered steps (name, class, labels, inputs, outputs).

| Option | Description |
|---|---|
| `--status` | Add `total_idx_count` / `changed_idx_count` when `get_status` is implemented. |

### `step run`

Run filtered steps (same loop / callback options as `datapipe run`).

| Option | Default | Description |
|---|---|---|
| `--loop` | off | Continuous run. |
| `--loop-delay` | `30` | Sleep between loops. |
| `--no-callbacks` | off | Skip entry-point run callbacks. |

### `step run-idx`

Run one index on filtered **batch transform** steps.

```bash
datapipe step --name my_step run-idx "id=1,other=x"
```

Argument `IDX`: comma-separated `col=value` pairs → single-row `IndexDF`.

### `step run-changelist`

Drive a start batch step by full process ids, then propagate each chunk’s changelist through the filtered steps.

| Option | Default | Description |
|---|---|---|
| `--start-step` | first filtered step | Name prefix; must resolve to exactly one `BaseBatchTransformStep`. |
| `--chunk-size` | step default | Override chunk size for the start step’s id generator. |
| `--loop` | off | Repeat. |
| `--loop-delay` | `1` | Sleep between full passes. |

### `step fill-metadata`

For batch transform steps: insert all current process indexes into transform meta without running `func`.

### `step reset-metadata`

Mark all transform-meta rows unprocessed for filtered batch steps.

---

## `datapipe table`

### `table list`

Print sorted catalog table names.

### `table migrate-transform-tables`

Run v0.13 transform-table migration for filtered steps (`--name` / `--labels` same semantics as `step`).

---

## `datapipe db`

### `db create-all`

Create known SQL tables; optionally sync schema.

| Option | Description |
|---|---|
| `--force-recreate` | Drop all known tables first (**destructive**). |

Behavior:

1. Refuses if the DB is Alembic-managed (`refuse_if_alembic_managed`).
2. `ensure_db_schema` + `datapipe.db_create_all` entry points.
3. Optional drop, then `sqla_metadata.create_all`.
4. If `datapipe-core[alembic]` is installed, `sync_sqla_metadata` (ADD COLUMN / ALTER); otherwise prints a skip message.

---

## `datapipe lint`

Check table consistency (e.g. delete_ts vs update/process, data without meta).

| Option | Default | Description |
|---|---|---|
| `--tables` | `*` | Comma-separated catalog names, or `*` for all. |
| `--fix` | off | Attempt automatic fixes where implemented. |

---

## `datapipe api` (requires `datapipe-app`)

Registered via the `datapipe.cli` entry point when `datapipe-app` is installed. Serves Ops HTTP + UI with uvicorn.

```bash
datapipe --pipeline app:app api
datapipe --pipeline app:app api --host 127.0.0.1 --port 8000
```

| Option | Default | Description |
|---|---|---|
| `--host` | `0.0.0.0` | Bind address. |
| `--port` | `8000` | Bind port. |

Wraps the loaded app in `DatapipeAPI` (unless it already is one) and respects the global `--executor`. OpenAPI UI is typically at `/docs` when the server is up. Dashboard at `/`.

See [Install and run Ops](../ops/install-and-run.md) and [DatapipeApp and API](../ops/datapipe-app.md).

---

## Labels (quick)

```bash
datapipe step --labels=stage=etl,team=ml list
datapipe step --name=detect,embed run
```

See [Filter steps by labels](../how-to/filter-by-labels.md).

### See also

- [Extend the CLI](../how-to/extend-cli.md)
- [DatapipeApp](./pipeline-catalog.md#datapipeapp)
- [Executors](./executors.md)
