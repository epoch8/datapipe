# BatchTransform / DatatableBatchTransform

Incremental DataFrame transforms driven by transform-meta change detection.

Module: `datapipe.step.batch_transform`

Declarative types: [`BatchTransform`](#batchtransform), [`DatatableBatchTransform`](#datatablebatchtransform).

Runtime base: [`BaseBatchTransformStep`](#basebatchtransformstep) (extends [`ComputeStep`](../compute-step.md)).

---

## Declarative vs runtime

| Declarative (`PipelineStep`) | Runtime (`ComputeStep`) | Notes |
|---|---|---|
| `BatchTransform` | `BatchTransformStep` | Preloads input DataFrames; injects `ds` / `idx` / `run_config` when declared in `func` |
| `DatatableBatchTransform` | `DatatableBatchTransformStep` | Passes `DataTable` list + `idx` to `func` |
| — | `BaseBatchTransformStep` | Shared incremental engine (meta, batching, store/delete) |

Build path: `PipelineStep.build_compute(ds, catalog)` → one runtime step per declarative step.

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

### Fields

| Name | Type | Default | Description |
|---|---|---|---|
| `func` | `BatchTransformFunc` | — | Callable; positional args are input DataFrames in `inputs` order. |
| `inputs` | `list[PipelineInput]` | — | Input tables. |
| `outputs` | `list[PipelineOutput]` | — | Output tables. |
| `chunk_size` | `int` | `1000` | Indexes per batch. |
| `name` | `str \| None` | `None` | Step name; default from mungled func + I/O names. |
| `kwargs` | `dict \| None` | `None` | Extra keyword args merged into the call. |
| `transform_keys` | `list[str] \| None` | `None` | Explicit transform grain; inferred when unset. |
| `labels` | `Labels \| None` | `None` | CLI / filter labels. |
| `executor_config` | `ExecutorConfig \| None` | `None` | Ray / parallel resources. |
| `filters` | `LabelDict \| Callable` | `None` | Merged into `RunConfig.filters` at run time. |
| `order_by` | `list[str] \| None` | `None` | Processing order columns. |
| `order` | `"asc" \| "desc"` | `"asc"` | Sort direction for `order_by`. |

### Injected kwargs

If `func` declares these parameters, Datapipe injects them:

| Parameter | Value |
|---|---|
| `ds` | Active `DataStore` |
| `idx` | Current batch `IndexDF` |
| `run_config` | Current `RunConfig` |

If `idx` is **not** in the signature and all input DataFrames are empty, the batch returns `None` (delete path for outputs). Declaring `idx` forces the call even when inputs are empty.

### `BatchTransform.build_compute`

```python
def build_compute(self, ds: DataStore, catalog: Catalog) -> list[ComputeStep]: ...
```

### Returns

Single-element list containing `BatchTransformStep`.

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

### Behavior notes

- Does **not** expose `filters` / `order_by` / `order` on the dataclass (unlike `BatchTransform`).
- You load/store data yourself via `input_dts`; return values still go through `store_batch_result`.
- Prefer `BatchTransform` unless you need custom reads or non-DataFrame IO.

### See also

- [DatatableTransform](./datatable-transform.md) — non-incremental, whole-table

---

## `BaseBatchTransformStep`

When to use: Runtime incremental batch engine shared by `BatchTransformStep` and `DatatableBatchTransformStep`. Implements [`ComputeStep`](../compute-step.md) run modes.

### Overview

| Member | Type | Description |
|---|---|---|
| `name` | `str` | Step name. |
| `input_dts` | `list[ComputeInput]` | Input bindings. |
| `output_dts` | `list[ComputeOutput]` | Output bindings. |
| `output_specs` | `Sequence[ComputeOutput]` | Same as `output_dts`; used when storing results. |
| `chunk_size` | `int` | Default batch size. |
| `meta` | `TransformMeta` | Transform-meta for `{name}_meta`. |
| `transform_keys` | `list[str]` | Processing grain. |
| `transform_schema` | `MetaSchema` | SQL schema for transform keys. |
| `filters` | `LabelDict \| Callable \| None` | Step-level run filters. |
| `order_by` | `list[str] \| None` | Meta query ordering. |
| `order` | `"asc" \| "desc"` | Sort direction. |

---

## `BaseBatchTransformStep.__init__`

```python
def __init__(
    self,
    ds: DataStore,
    name: str,
    input_dts: Sequence[ComputeInput],
    output_dts: Sequence[ComputeOutput],
    transform_keys: list[str] | None = None,
    chunk_size: int = 1000,
    labels: Labels | None = None,
    executor_config: ExecutorConfig | None = None,
    filters: LabelDict | Callable[[], LabelDict] | None = None,
    order_by: list[str] | None = None,
    order: Literal["asc", "desc"] = "asc",
) -> None: ...
```

### Behavior notes

- Creates transform meta via `ds.meta_plane.create_transform_meta(name=f"{name}_meta", ...)`.
- Non-list `transform_keys` are coerced to `list`.

---

## `BaseBatchTransformStep.get_status`

```python
def get_status(self, ds: DataStore) -> StepStatus: ...
```

### Returns

`StepStatus` with `total_idx_count=meta.get_metadata_size()` and `changed_idx_count=meta.get_changed_idx_count(ds)`.

---

## `BaseBatchTransformStep.get_full_process_ids`

```python
def get_full_process_ids(
    self,
    ds: DataStore,
    chunk_size: int | None = None,
    run_config: RunConfig | None = None,
) -> tuple[int, Generator[IndexDF, None, None]]: ...
```

### Behavior notes

- Merges step `filters` into `run_config` via `_apply_filters_to_run_config`.
- Delegates to `meta.get_full_process_ids` with `chunk_size or self.chunk_size`.

---

## `BaseBatchTransformStep.get_change_list_process_ids`

```python
def get_change_list_process_ids(
    self,
    ds: DataStore,
    change_list: ChangeList,
    run_config: RunConfig | None = None,
) -> tuple[int, Generator[IndexDF, None, None]]: ...
```

### Behavior notes

- Applies step filters, then `meta.get_change_list_process_ids(..., chunk_size=self.chunk_size)`.

---

## `BaseBatchTransformStep.get_batch_input_dfs`

When to use: Load input DataFrames for one batch index.

```python
def get_batch_input_dfs(
    self,
    ds: DataStore,
    idx: IndexDF,
    run_config: RunConfig | None = None,
) -> list[DataDF]: ...
```

### Returns

`list[DataDF]` — one per `input_dts` entry, via `inp.dt.get_data(meta.transform_idx_to_table_idx(idx, inp.keys))`.

---

## `BaseBatchTransformStep.process_batch_dfs`

When to use: Override in subclasses to run the user transform on loaded frames.

```python
def process_batch_dfs(
    self,
    ds: DataStore,
    idx: IndexDF,
    input_dfs: list[DataDF],
    run_config: RunConfig | None = None,
) -> TransformResult: ...
```

### Raises

| Exception | When |
|---|---|
| `NotImplementedError` | Base class. |

### Behavior notes

- `BatchTransformStep` calls `func(*input_dfs, **injected_kwargs)`.

---

## `BaseBatchTransformStep.process_batch_dts`

When to use: Template method — load inputs, optionally skip empty batches, call `process_batch_dfs`.

```python
def process_batch_dts(
    self,
    ds: DataStore,
    idx: IndexDF,
    run_config: RunConfig | None = None,
) -> TransformResult | None: ...
```

### Returns

Transform result, or `None` when all input frames are empty (base implementation).

### Behavior notes

- `BatchTransformStep` overrides to honor `idx` in `func` signature when inputs are empty.

---

## `BaseBatchTransformStep.process_batch`

When to use: Process one index batch end-to-end (transform + store + meta update).

```python
def process_batch(
    self,
    ds: DataStore,
    idx: IndexDF,
    run_config: RunConfig | None = None,
) -> ChangeList: ...
```

### Returns

`ChangeList` — output table changes on success; empty on handled error.

### Raises

| Exception | When |
|---|---|
| `Exception` | Any batch error when `run_config.fail_fast` is `True`. |

### Behavior notes

- On success: `store_batch_result`.
- On failure: logs via `event_logger`, `store_batch_err`, returns empty `ChangeList` unless `fail_fast`.

---

## `BaseBatchTransformStep.store_batch_result`

When to use: Persist transform outputs and mark transform meta success.

```python
def store_batch_result(
    self,
    ds: DataStore,
    idx: IndexDF,
    output_dfs: TransformResult | None,
    process_ts: float,
    run_config: RunConfig | None = None,
) -> ChangeList: ...
```

### Parameters

| Name | Type | Description |
|---|---|---|
| `output_dfs` | `TransformResult \| None` | One or more output DataFrames, or `None` to delete outputs for the batch. |

### Returns

`ChangeList` — per-output-table changed indexes from `store_chunk` or deletes.

### Behavior notes

- Normalizes single vs list/tuple outputs to match `output_dts` length.
- Maps batch index to output keys via `_transform_idx_to_output_idx`.
- `None` result: deletes existing output rows for the batch index.
- Calls `meta.mark_rows_processed_success`.

---

## `BaseBatchTransformStep.store_batch_err`

When to use: Record batch failure in transform meta and event log.

```python
def store_batch_err(
    self,
    ds: DataStore,
    idx: IndexDF,
    e: Exception,
    process_ts: float,
    run_config: RunConfig | None = None,
) -> None: ...
```

### Behavior notes

- Logs error with idx records in event logger labels.
- Calls `meta.mark_rows_processed_error`.

---

## `BaseBatchTransformStep.fill_metadata`

When to use: Pre-populate transform-meta rows for all pending indexes (CLI `fill_metadata`).

```python
def fill_metadata(self, ds: DataStore, run_config: RunConfig | None = None) -> None: ...
```

### Behavior notes

- Iterates `get_full_process_ids(..., chunk_size=1000)` and `meta.insert_rows` per batch.
- Emits `run_config.callback.on_step_progress` when callback is set.

---

## `BaseBatchTransformStep.reset_metadata`

When to use: Mark all transform rows unprocessed.

```python
def reset_metadata(self, ds: DataStore) -> None: ...
```

### Behavior notes

- Calls `meta.mark_all_rows_unprocessed()` (ignores `ds` parameter).

---

## `BaseBatchTransformStep.run_full`

```python
def run_full(
    self,
    ds: DataStore,
    run_config: RunConfig | None = None,
    executor: Executor | None = None,
) -> None: ...
```

### Behavior notes

- Defaults to `SingleThreadExecutor`.
- Adds `step_name` label to `run_config`.
- Early return when batch count is `0`.
- Delegates batch loop to `executor.run_process_batch(..., process_fn=self.process_batch)`.
- Logs `event_logger.log_step_full_complete`.

---

## `BaseBatchTransformStep.run_changelist`

```python
def run_changelist(
    self,
    ds: DataStore,
    change_list: ChangeList,
    run_config: RunConfig | None = None,
    executor: Executor | None = None,
) -> ChangeList: ...
```

### Returns

Aggregated `ChangeList` from executor (merged batch results).

### Behavior notes

- Returns empty `ChangeList` when zero batches.

---

## `BaseBatchTransformStep.run_idx`

```python
def run_idx(
    self,
    ds: DataStore,
    idx: IndexDF,
    run_config: RunConfig | None = None,
    executor: Executor | None = None,
) -> ChangeList: ...
```

### Returns

`ChangeList` from a single `process_batch` call.

### Behavior notes

- Accepts `executor` for API symmetry but calls `process_batch` directly (no executor).

---

## `BaseBatchTransformStep._apply_filters_to_run_config`

When to use: Internal — merge step `filters` (dict or callable) into run config.

```python
def _apply_filters_to_run_config(self, run_config: RunConfig | None = None) -> RunConfig | None: ...
```

### Behavior notes

- Step filters are deep-copied and updated with existing `run_config.filters` (step filters first, run config wins on key collision).

---

## `BaseBatchTransformStep._transform_idx_to_output_idx`

When to use: Static helper — map transform batch index to output table primary keys.

```python
@staticmethod
def _transform_idx_to_output_idx(
    idx: IndexDF,
    output_spec: ComputeOutput,
) -> IndexDF | None: ...
```

### Returns

`IndexDF` with output primary-key columns, or `None` when no mappable columns.

---

## See also

- [ComputeStep](../compute-step.md)
- [TransformMeta](../meta.md)
- [Required / InputSpec / OutputSpec](../types.md)
- [Executors](../executors.md)
- [RunConfig](../run-config.md)
- [Incremental processing](../../concepts/incremental-processing.md)
