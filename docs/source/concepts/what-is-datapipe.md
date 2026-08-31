# What is Datapipe?

Datapipe is a Python framework for **durable, incremental batch processing**. You define a graph of tables and transform steps once; on every run Datapipe processes **only the keys that need work**.

## Start here: the incremental idea

For each row, Datapipe tracks content (`hash`) and time (`update_ts`). For each step, it tracks when each key was last processed (`process_ts`). A key is dirty when input data is newer than the last successful process — or when the step never succeeded for that key.

**Identical rewrites are free:** same hash → no `update_ts` bump → your function does not run.

Visual walkthrough (insert / update / delete / unchanged):

→ **[Incremental Processing](./incremental-processing.md)**

## Building blocks

| Piece | Role |
|---|---|
| **Table + TableStore** | Named dataset + backend (SQL, files, Redis, …) |
| **Catalog** | Name → table map |
| **Pipeline steps** | Generate, sync external, batch-transform, or whole-table transform |
| **DataStore / DatapipeApp** | Runtime + optional Ops API |

Details: [Tables and TableStores](./tables-and-stores.md), [Pipeline Steps](./pipeline-steps.md).

## Durability

Processing state is written to a SQL metadata store after each successful batch. Crashes and restarts resume from dirty keys — no full replay.

## Batch orientation

Work units are `pd.DataFrame` chunks (`chunk_size` on `BatchTransform`), not single rows and not stream events. Tune memory and throughput independently.

## What Datapipe is not

- **Not a streaming engine** — no sub-second event windows; runs are triggered explicitly.
- **Not a distributed compute engine by default** — single-threaded executor out of the box; optional `RayExecutor`.
- **Not opinionated about storage** — bring any `TableStore` implementation.

## Next

1. [Installation](../getting-started/installation.md)
2. [Your First Pipeline](../getting-started/first-pipeline.md)
3. [Incremental Processing](./incremental-processing.md) — the four table-panel cases
