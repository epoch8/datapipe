# Concepts

Read these to understand **why** Datapipe is shaped the way it is.

## Reading order

1. [What is Datapipe?](./what-is-datapipe.md) — product overview
2. [Incremental Processing](./incremental-processing.md) — **start here** (six GIFs: insert, update, delete, unchanged, processed_idx, resurrection)
3. [Tables and TableStores](./tables-and-stores.md) — data vs meta layers
4. [Pipeline Steps](./pipeline-steps.md) — Generate, Transform, DatatableTransform
5. [Primary Keys and Transform Keys](./primary-keys.md) — PK vs transform grain
6. [Transform Grain](./transform-grain.md) — `transform_keys`, multi-input `max(update_ts)`, cross-product
7. [Soft Delete](./soft-delete.md) — hard delete data vs soft meta `delete_ts`, resurrection
8. [Output Cleanup and `processed_idx`](./processed-idx.md) — partial batch outputs, 1-to-N pitfalls
9. [The `idx` Parameter](./idx-parameter.md) — automatic delete path vs explicit `idx`
10. [BatchGenerate vs BatchTransform](./generate-vs-transform.md) — hash/stale vs step meta

For tasks, use [How-to Guides](../how-to/transform-files.md). For APIs, use [Reference](../reference/pipeline-catalog.md).
