# UpdateExternalTable

When to use: Refresh Datapipe metadata for a table whose **data** is written outside Datapipe (files, ETL, another process).

Module: `datapipe.step.update_external_table`

```python
class UpdateExternalTable(PipelineStep):
    def __init__(
        self,
        output: TableOrName,
        labels: Labels | None = None,
    ) -> None: ...
```

Builds a `DatatableTransformStep` with no inputs that calls `update_external_table`.

### Arguments

| Arg | Description |
|---|---|
| `output` | Table to sync (catalog name / ORM / `Table`). |
| `labels` | Optional CLI labels. |

### Behavior

1. Iterate `table.table_store.read_rows_meta_pseudo_df(...)`.
2. Hash each chunk; update meta for new/changed rows (`get_changes_for_store_chunk` + `meta.update_rows`). Does **not** rewrite store data.
3. Mark rows missing from this scan as deleted (`get_stale_idx` / `mark_rows_deleted`).

### Notes

- Requires a store that supports `read_rows_meta_pseudo_df` (file/DB/Elasticsearch typically yes).
- **Do not** use with stores that set `supports_read_meta_pseudo_df=False` — notably **Redis** and **Neo4j**. Use `BatchGenerate` or write through `DataTable.store_chunk` instead.
- Progress callbacks receive chunk counts when a `step` argument is accepted by the transform func.
- Step name is munged from `update_<table>` + I/O.

### See also

- [External sources how-to](../../how-to/external-sources.md)
- [TableStoreFiledir](../stores/filedir.md)
- [BatchGenerate](./batch-generate.md)
