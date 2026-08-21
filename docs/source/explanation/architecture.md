# Architecture overview

Datapipe splits **data**, **metadata**, and **execution**.

```text
┌───────────── your code ─────────────┐
│  Catalog  +  Pipeline (PipelineSteps)│
└───────────────┬─────────────────────┘
                │ build_compute()
                ▼
┌──────────── ComputeStep graph ──────┐
│  BatchTransformStep / Generate / …  │
└───────┬───────────────────┬─────────┘
        │ reads/writes      │ schedules via
        ▼                   ▼
┌──────────────┐    ┌──────────────────┐
│ TableStore   │    │ Meta plane (SQL) │
│ (data rows)  │    │ table_meta       │
│ DB/files/…   │    │ step_meta        │
└──────────────┘    └──────────────────┘
```

## Planes

| Plane | Responsibility |
|---|---|
| **Data** | Your `TableStore` backends hold business columns |
| **Meta** | `{table}_meta` hashes/timestamps; `{step}_meta` process state |
| **Compute** | `ComputeStep.run_full` / changelist finds dirty keys and calls your functions |

Design intent (MetaPlane interfaces): `libs/datapipe-core/design-docs/2025-12-meta-plane.md`.

## Incremental path (one sentence)

`store_chunk` updates data + meta → dirty keys where `update_ts > process_ts` → `BatchTransform` runs → writes outputs and marks step success.

Visual cases: [Incremental Processing](../concepts/incremental-processing.md).  
SQL detail: [Change Detection](./change-detection.md).  
Runtime call stack: [Compute Step Lifecycle](./compute-step-lifecycle.md).

## Ops plane (optional)

`datapipe-app` adds HTTP, run history, and UI **on top** of the same compute graph. It does not replace meta-plane incremental tracking. See [Ops](../ops/index.md).

## Source diagrams

Editable diagrams live next to these pages:

- `architecture.drawio` (this overview — export PNG if you need a static asset in a PR)
- `transformation-lifecycle.drawio` / `transformation-lifecycle.png` (step run internals)
