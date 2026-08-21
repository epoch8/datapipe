# Ops: App, UI, and Observability

Datapipe Ops wraps a pipeline in a FastAPI app with a dashboard: pipeline graph, tables, runs, and (optionally) ML metrics.

| Package | Role |
|---|---|
| `datapipe-app` | REST API, CLI hooks, observability |
| `datapipe-ui` | Core Ops SPA (graph, runs, tables) |
| `datapipe-ui-ml` | ML plugin SPA |
| `datapipe-app-ml-ops` | ML API / ops-specs backend |

This section is for running and operating pipelines. Core incremental concepts stay in [Incremental Processing](../concepts/incremental-processing.md).

## Guides in this section

1. [Install and run Ops](./install-and-run.md) — extras, UI build, `datapipe api`
2. [DatapipeApp and API](./datapipe-app.md) — wiring `ds` / `catalog` / `pipeline`
3. [Ops UI walkthrough](./ui-walkthrough.md) — what you see in the dashboard
4. [Observability and run logs](./observability.md) — runs store, ClickHouse logs

Hands-on example: `examples/datapipe_app/` (see that folder's README).
