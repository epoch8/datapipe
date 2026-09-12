# APIs

Part of [Cloud connectivity architecture](00-Overview.md).

The three APIs are derived from the Datapipe Ops API v1alpha3
(`/api/v1alpha3` in `datapipe-app`, currently on the `feat/cancel-token`
branch). v1alpha3 is one FastAPI app in one process: it reads pipeline state,
starts runs as threads of the same process, and records runs and their logs in
the observability database. This design keeps its paths and payloads where
possible and splits its routes by the component that serves them.

## Common rules

* **Pipeline-scoped paths.** Every route lives under
  `/api/v1alpha3/pipelines/{pipeline_id}/`. An agent can serve several
  pipelines and routes requests by `pipeline_id`. v1alpha3 routes that are not
  scoped yet (`/graph`, `/get-table-data`, `/runs`, …) move under this prefix.
* **Same API in both scenarios.** Paths and payloads are identical in Local and
  Cloud. In Cloud the Dashboard API exposes them under a project prefix
  (`/projects/{project_id}/…`). It sends visualization and run requests
  through the gateway to the environment with the rest of the path unchanged,
  and answers observability requests itself.
* **Polling, no websockets.** Live updates (run progress, log tail) use
  cursor-based polling, which works the same over the Cloud connection. (The
  agent–gateway connection is a WebSocket, but it only carries these HTTP
  requests; see [02-Agent-and-Gateway.md](02-Agent-and-Gateway.md).)

## Visualization API

Served by the pipeline instance. It needs pipeline code: the graph comes from
the pipeline steps, table data from the catalog's table stores.

| Route (under `/pipelines/{pipeline_id}`) | v1alpha3 today | Returns |
|---|---|---|
| `GET /` | `GET /pipelines/{pipeline_id}` | stages, stage edges, label graph, plugin enrichments |
| `GET /graph?stage=` | `GET /graph` | tables and steps, with total and changed row counts |
| `POST /get-table-data` | `POST /get-table-data` | a page of table rows (filters, focus, order, paging) |
| `POST /get-transform-data` | `POST /get-transform-data` | a page of transform metadata rows |
| `GET /tables/{table}/size` | same, unscoped | table row count |
| `GET /transforms/{transform}/meta-size` | same, unscoped | transform metadata row count |
| `POST /transforms/{transform}/reset-metadata` | same | marks all rows of a transform for reprocessing |
| plugin routes: `/ops-specs/*`, `/ops-pages/*`, `/metrics/*`, `/training/*` | same | ML plugin views and edits |

The rule: **the visualization API never executes pipeline steps.** It may make
cheap edits to pipeline state — reset transform metadata, edit plugin
configuration such as training experiments — because those need pipeline code
but no compute. Anything that executes steps is a run and goes through the run
API. Three things in v1alpha3 do not fit this split and change:

* `WS /ws/transform/{transform}/run-status` runs a single transform inside the
  API process. It is replaced by `POST /runs` selecting one transform, with
  progress from `GET /runs/{run_id}`.
* Plugin routes that launch work (training requests, dataset freeze) call
  `run_steps` in a thread of the API process. They call the run API instead.
* `GET /pipelines/{pipeline_id}` also returns run data: recent runs, last
  error, health. These come from the run API; the dashboard combines both.

## Run API

Served by the agent in Cloud and by the pipeline instance in Local.
v1alpha3 already has these routes:

| Route (under `/pipelines/{pipeline_id}`) | v1alpha3 today | Purpose |
|---|---|---|
| `GET /runs` | `GET /runs?pipeline_id=` | list runs: filters (status, stage, trigger, search, time range), paging, sorting, counts by status |
| `GET /runs/{run_id}` | same, unscoped | run status, error, scope, and per-step status with processed/total rows |
| `POST /runs` | same, unscoped | start a run |
| `POST /runs/{run_id}/stop` | same, unscoped | stop a run |
| `GET /stages/{stage}/recent-runs` | same | last runs that touched a stage |

**Run records.** v1alpha3 stores runs in the observability database
(`datapipe_api__runs`, `datapipe_api__run_steps`). They are written by the
process that executes the steps, through a `RunCallback` — this includes
`datapipe run` from the CLI, via the `datapipe.run_callbacks` entry point. We
keep this: the run worker records its own progress, and the run API reads it
from the database. Runs started outside the run API (cron, a shell) show up in
the history too. The agent needs a connection to the observability database,
but no pipeline code.

**Statuses.** v1alpha3 has `running`, `completed`, `failed`, `interrupted`.
We add `pending`: the run is accepted but its process has not started
recording yet.

Changes from v1alpha3:

1. **Runs are separate processes.** v1alpha3 runs steps in a thread of the API
   process, or synchronously inside the request with `background: false`. Here
   every run is its own process, started by the component that serves the run
   API. `background` is dropped; `POST /runs` always returns immediately.
2. **Selection.** `StartRunRequest` has `labels`. We add `transform` (run a
   single transform, replacing the websocket) and `filters` (primary-key
   filters, which the ML plugin already passes to `run_steps`). They are
   passed to the run process.
3. **Idempotent start.** The caller may pass `run_id`. A repeated `POST /runs`
   with the same `run_id` returns the existing run instead of starting a
   second one. Cloud needs this: a request can be retried after the
   connection drops.
4. **Stop signals the process.** v1alpha3 stops a run through an in-process
   `CancelToken`; if the run is not in the same process, it only changes the
   recorded status. Here the run API sends the run process a signal, the
   process cancels its token, and it is killed if it does not exit within a
   grace period.
5. **Exit code catches crashes.** If a run process exits without recording a
   final status (pipeline import error, OOM kill), the run API marks the run
   `failed`. The v1alpha3 reconciler, which marks leftover `running` runs
   `interrupted` on API startup, stays for runs whose process is gone after a
   restart.

## Observability API

Served by the pipeline instance in Local and by the cloud in Cloud.

| Route (under `/pipelines/{pipeline_id}`) | v1alpha3 today | Purpose |
|---|---|---|
| `GET /runs/{run_id}/logs?after=&limit=` | same, unscoped | log lines of a run (seq, time, level, message) and `last_seq` / `max_seq` for tailing |

**Logs in v1alpha3.** The process executing a run captures its stdout, stderr
and logging (`capture_run_output`) and writes lines to a run logs backend:
the observability SQL database, ClickHouse, or memory. The logs route reads
the backend with a cursor (`after`), so tailing is polling.

**Local.** Unchanged: the run process writes to the backend, the pipeline
instance reads from it.

**Cloud.** Logs live in the cloud. The agent reads new lines from the run
logs backend and forwards them over its outbound connection; the cloud stores
them and serves the logs route from its own storage. Reading the backend,
rather than only the processes it started, means runs started outside the
agent (cron, a shell) are forwarded too. Lines carry their `seq`, so after a
reconnect the agent resumes from the last line the cloud has, and the cloud
drops duplicates.

Changes in both scenarios:

1. **No in-memory backend outside tests.** The writer (run process) and the
   reader (agent or pipeline instance) are different processes, so the
   in-memory backend cannot work.
2. **Process output fallback.** Output printed before capture starts — for
   example, an error while importing the pipeline — never reaches the backend.
   The component that starts a process keeps its raw stdout and stderr, and
   they are served when a run has no captured logs. In Cloud the agent
   forwards them to the cloud together with captured lines.

**Metrics and traces.** v1alpha3 has no API for them. `datapipe-app` exposes
Prometheus metrics (total and changed row counts per transform) at
`/-/metrics` of the API process, and emits OpenTelemetry spans to whatever
exporter the environment configures.

## Open questions

* **Metrics and traces.** Forward them to the cloud like logs and serve them
  through the observability API, or keep exporting to the environment's own
  monitoring stack and only link to it from the dashboard?
* **Capabilities.** v1alpha3 `/capabilities` mixes plugin flags (ML metrics,
  ML training), which need pipeline code, with run-level flags (run logs
  configured). In Cloud it should be answerable without starting a pipeline
  instance.
* **Concurrent runs.** What happens when a second run of the same pipeline
  starts while the first is running and their steps overlap: queue, reject, or
  allow?
