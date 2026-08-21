# BatchGenerate

When to use: Seed or refresh output tables from a **generator** that yields DataFrame batches (no inputs).

Module: `datapipe.step.batch_generate`

```python
@dataclass
class BatchGenerate(PipelineStep):
    func: BatchGenerateFunc  # Callable[..., Iterator[TransformResult]]
    outputs: list[TableOrName]
    name: str | None = None
    kwargs: dict | None = None
    labels: Labels | None = None
    delete_stale: bool = True
```

Builds a `DatatableTransformStep` that runs `do_batch_generate` (`check_for_changes=False`).

### Arguments

| Arg | Description |
|---|---|
| `func` | **Must** be a generator function (`yield` batches). Each yield is one `DataDF` or a tuple/list of DataFrames matching `outputs`. |
| `outputs` | Output table names / ORM / `Table`. |
| `name` | Step name; default munged from func + outputs. |
| `kwargs` | Passed into `func` (plus injected `ds` if declared). |
| `labels` | CLI labels. |
| `delete_stale` | If `True` (default), after the generator finishes, delete rows with `process_ts` older than the run start. |

### Injected kwargs

| Parameter | Value |
|---|---|
| `ds` | Active `DataStore` (if present in signature) |

### Notes

- Since v0.8.0, non-generator `func` raises.
- Each yielded chunk is written with `DataTable.store_chunk` (no `processed_idx`).
- Exceptions during `next(iterable)` are logged; the helper returns without re-raising (init failures still raise).
- Implemented on top of `DatatableTransformStep`, so it runs as a full-table step (not changelist-incremental).

### Example

```python
def generate_words():
    yield pd.DataFrame({"id": [1, 2], "text": ["a", "bb"]})

BatchGenerate(func=generate_words, outputs=["words"])
```

### See also

- [UpdateExternalTable](./update-external-table.md) — sync meta for externally written data
- [Pipeline steps](../../concepts/pipeline-steps.md)
