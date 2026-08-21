# DatatableTransform

When to use: Non-incremental, whole-table work where the function needs `DataTable` handles (training, global side effects, custom orchestration).

Module: `datapipe.step.datatable_transform`

```python
@dataclass
class DatatableTransform(PipelineStep):
    func: DatatableTransformFunc
    inputs: list[TableOrName]
    outputs: list[TableOrName]
    check_for_changes: bool = True
    kwargs: dict[str, Any] | None = None
    labels: Labels | None = None
```

Builds a `DatatableTransformStep`.

### Arguments

| Arg | Description |
|---|---|
| `func` | See signature below. Returns `None`; you read/write tables yourself. |
| `inputs` | Input table names / ORM / `Table`. |
| `outputs` | Output table names / ORM / `Table`. |
| `check_for_changes` | Reserved; change-skip logic is currently commented out — step always runs. |
| `kwargs` | Passed as `kwargs=` to `func`. |
| `labels` | CLI labels. |

### `func` signature

```python
def func(
    ds: DataStore,
    input_dts: list[DataTable],
    output_dts: list[DataTable],
    run_config: RunConfig | None,
    kwargs: dict[str, Any] | None = None,
) -> None: ...
```

If the signature includes `step`, the runtime `ComputeStep` is injected as well (used by `UpdateExternalTable`).

### Notes

- No transform-meta / changelist participation.
- Exceptions are logged via `event_logger`; the step does not re-raise after logging.
- Prefer `BatchTransform` when row-level incremental processing is enough.

### See also

- [BatchTransform](./batch-transform.md)
- [DatatableBatchTransform](./batch-transform.md#datatablebatchtransform)
- [Pipeline steps](../../concepts/pipeline-steps.md)
