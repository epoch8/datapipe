# Steps

Declarative `PipelineStep` types and what they become at runtime.

| Step | Incremental? | When to use |
|---|---|---|
| [BatchTransform](./batch-transform.md) | Yes (row / transform-key) | Stateless DataFrame → DataFrame work |
| [DatatableBatchTransform](./batch-transform.md#datatablebatchtransform) | Yes | Same as above, but `func` gets `DataTable` handles + `idx` |
| [BatchGenerate](./batch-generate.md) | Seeds outputs | Generator that `yield`s DataFrames into output tables |
| [UpdateExternalTable](./update-external-table.md) | Syncs meta | Data written outside Datapipe; refresh hashes / delete stale |
| [DatatableTransform](./datatable-transform.md) | No (whole tables) | Global jobs that need full `DataTable` access |

### See also

- [Pipeline steps (concepts)](../../concepts/pipeline-steps.md)
- [Pipeline / Catalog](../pipeline-catalog.md)
- [Compute step lifecycle](../../explanation/compute-step-lifecycle.md)
