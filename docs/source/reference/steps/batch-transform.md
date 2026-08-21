# BatchTransform / DatatableBatchTransform

Incremental DataFrame transforms driven by transform-meta change detection.

Module: `datapipe.step.batch_transform`

---

## `BatchTransform`

When to use: Per-batch DataFrame → DataFrame work with automatic change tracking.

```python
@dataclass
class BatchTransform(PipelineStep):
    func: BatchTransformFunc
    inputs: list[PipelineInput]
    outputs: list[PipelineOutput]
    chunk_size: int = 1000
    name: str | None = None
    kwargs: dict[str, Any] | None = None
    transform_keys: list[str] | None = None
    labels: Labels | None = None
    executor_config: ExecutorConfig | None = None
    filters: LabelDict | Callable[[], LabelDict] | None = None
    order_by: list[str] | None = None
    order: Literal["asc", "desc"] = "asc"
```

Builds a `BatchTransformStep` (`BaseBatchTransformStep`).

### Arguments

| Arg | Description |
|---|---|
| `func` | Callable. Positional args are input DataFrames (same order as `inputs`). May return one `DataDF` or a list/tuple matching `outputs`. |
| `inputs` | Input tables: `str` / ORM / `Table` / `InputSpec` / `Required`. |
| `outputs` | Output tables: `str` / ORM / `Table` / `OutputSpec`. |
| `chunk_size` | Indexes per batch (default `1000`). |
| `name` | Compute step name; default is a munged name from func + I/O. |
| `kwargs` | Extra keyword args merged into the call. |
| `transform_keys` | Explicit transform grain; otherwise inferred from input/output primary keys. |
| `labels` | CLI / filter labels `[(k, v), ...]`. |
| `executor_config` | Resources / parallelism for `RayExecutor`. |
| `filters` | Restrict indexes (`LabelDict` or callable returning one). Merged into `RunConfig.filters`. |
| `order_by` | Optional columns for processing order. |
| `order` | `"asc"` or `"desc"`. |

### Injected kwargs

If `func` declares these parameters, Datapipe injects them:

| Parameter | Value |
|---|---|
| `ds` | Active `DataStore` |
| `idx` | Current batch `IndexDF` |
| `run_config` | Current `RunConfig` |

If `idx` is **not** in the signature and all input DataFrames are empty, the batch returns `None` (delete path for outputs). Declaring `idx` forces the call even when inputs are empty.

### Notes

- Creates transform meta via `ds.meta_plane.create_transform_meta`.
- `run_full` / `run_changelist` use an `Executor` (`SingleThreadExecutor` if omitted).
- On exception: logs and marks error unless `run_config.fail_fast`.
- Output storage uses `store_chunk` with mapped `processed_idx`; `None` result deletes existing rows for the batch index.

### Example

```python
def count_chars(words: pd.DataFrame) -> pd.DataFrame:
    return words.assign(n=words["text"].str.len())

BatchTransform(
    func=count_chars,
    inputs=["words"],
    outputs=["word_lengths"],
    chunk_size=500,
)
```

### See also

- [Required / InputSpec / OutputSpec](../types.md)
- [Executors](../executors.md)
- [Incremental processing](../../concepts/incremental-processing.md)

---

## `DatatableBatchTransform`

When to use: Same incremental machinery, but `func` receives `DataTable` list + `idx` instead of preloaded DataFrames.

```python
@dataclass
class DatatableBatchTransform(PipelineStep):
    func: DatatableBatchTransformFunc
    inputs: list[PipelineInput]
    outputs: list[PipelineOutput]
    name: str | None = None
    chunk_size: int = 1000
    transform_keys: list[str] | None = None
    kwargs: dict | None = None
    labels: Labels | None = None
    executor_config: ExecutorConfig | None = None
```

### `func` signature

```python
def func(
    ds: DataStore,
    idx: IndexDF,
    input_dts: list[DataTable],
    run_config: RunConfig | None = None,
    kwargs: dict[str, Any] | None = None,
) -> TransformResult: ...
```

### Notes

- Does **not** expose `filters` / `order_by` / `order` on the dataclass (unlike `BatchTransform`).
- You load/store data yourself via `input_dts` / return values still go through `store_batch_result` like `BatchTransform`.
- Prefer `BatchTransform` unless you need custom reads or non-DataFrame IO around the same change tracking.

### See also

- [DatatableTransform](./datatable-transform.md) — non-incremental, whole-table
