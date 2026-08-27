# Steps

Declarative `PipelineStep` types and their runtime `ComputeStep` implementations.

Module index: `datapipe.step.*`, `datapipe.compute`

---

## Declarative step catalog

| Step | Incremental? | When to use |
|---|---|---|
| [BatchTransform](./batch-transform.md) | Yes (row / transform-key) | Stateless DataFrame → DataFrame work |
| [DatatableBatchTransform](./batch-transform.md#datatablebatchtransform) | Yes | Same as above, but `func` gets `DataTable` handles + `idx` |
| [BatchGenerate](./batch-generate.md) | Seeds outputs | Generator that `yield`s DataFrames into output tables |
| [UpdateExternalTable](./update-external-table.md) | Syncs meta | Data written outside Datapipe; refresh hashes / delete stale |
| [DatatableTransform](./datatable-transform.md) | No (whole tables) | Global jobs that need full `DataTable` access |

---

## Runtime compute layer

| Runtime type | Module | Reference |
|---|---|---|
| [`ComputeStep`](../compute-step.md) | `datapipe.compute` | Base runtime node: validation, full/changelist/idx runs |
| [`ComputeInput` / `ComputeOutput`](../compute-step.md) | `datapipe.compute` | Input/output table bindings and key maps |
| [`StepStatus`](../compute-step.md) | `datapipe.compute` | Status snapshot from `get_status` |
| [`BaseBatchTransformStep`](./batch-transform.md#basebatchtransformstep) | `datapipe.step.batch_transform` | Incremental batch engine for transform steps |

Build: each `PipelineStep.build_compute(ds, catalog)` returns one or more `ComputeStep` instances. See [`build_compute`](../pipeline-catalog.md#build_compute).

---

## Declarative → runtime mapping

| Declarative (`PipelineStep`) | Runtime (`ComputeStep`) | Notes |
|---|---|---|
| `BatchTransform` | `BatchTransformStep` | Extends `BaseBatchTransformStep`; calls user `func` on DataFrames |
| `DatatableBatchTransform` | `DatatableBatchTransformStep` | Extends `BaseBatchTransformStep`; passes `DataTable` list to `func` |
| `BatchGenerate` | `BatchGenerateStep` | Yields chunks into output tables |
| `UpdateExternalTable` | `UpdateExternalTableStep` | Refreshes meta from external writes |
| `DatatableTransform` | `DatatableTransformStep` | Full-table pass, no transform meta |

All runtime steps share the [`ComputeStep`](../compute-step.md) contract: `run_full`, `run_changelist`, `run_idx`, `get_status`, `validate`.

---

## Execution modes

| Mode | Entry point | Uses |
|---|---|---|
| Full pipeline | `run_pipeline` / `run_steps` | Each step `run_full` |
| Changelist | `run_changelist` / `run_steps_changelist` | `BaseBatchTransformStep.run_changelist` propagates `ChangeList` |
| Single index | CLI `run_idx`, `ComputeStep.run_idx` | One batch |

Configure runs with [`RunConfig`](../run-config.md) (filters, `fail_fast`, callbacks).

---

## See also

- [Pipeline steps (concepts)](../../concepts/pipeline-steps.md)
- [Pipeline / Catalog](../pipeline-catalog.md)
- [Compute step lifecycle](../../explanation/compute-step-lifecycle.md)
- [TableMeta / TransformMeta](../meta.md)
