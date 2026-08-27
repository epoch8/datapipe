# ComputeStep

Runtime compute-graph node: binds input/output tables, validates join keys, and defines full / changelist / index-scoped execution.

Module: `datapipe.compute`

Related types: [`ComputeInput`](#computeinput), [`ComputeOutput`](#computeoutput), [`StepStatus`](#stepstatus).

---

## `StepStatus`

When to use: Snapshot returned by `ComputeStep.get_status` for observability and CLI.

```python
@dataclass
class StepStatus:
    name: str
    total_idx_count: int
    changed_idx_count: int
```

### Fields

| Name | Type | Description |
|---|---|---|
| `name` | `str` | Step name. |
| `total_idx_count` | `int` | Total transform-meta rows (or step-specific total). |
| `changed_idx_count` | `int` | Indexes currently needing processing. |

---

## `ComputeInput`

When to use: Describe one input `DataTable` for a runtime step, including join semantics and optional key remapping.

```python
@dataclass
class ComputeInput:
    dt: DataTable
    join_type: Literal["inner", "full"] = "full"
    keys: dict[str, str] | None = None
```

### Fields

| Name | Type | Default | Description |
|---|---|---|---|
| `dt` | `DataTable` | — | Input table handle. |
| `join_type` | `Literal["inner", "full"]` | `"full"` | How this input participates in transform-meta joins. `"inner"` for required inputs (`Required` in declarative steps). |
| `keys` | `dict[str, str] \| None` | `None` | Map `{transform_key: table_column}`. Reads `table_column` from meta when resolving transform indexes. |

### `ComputeInput.primary_keys`

```python
@property
def primary_keys(self) -> list[str]: ...
```

### Returns

List of transform-side key names: keys of `keys` if set, else `dt.primary_keys`.

### `ComputeInput.primary_schema`

```python
@property
def primary_schema(self) -> MetaSchema: ...
```

### Returns

SQLAlchemy columns for transform keys. When `keys` is set, builds aliased columns from the mapped source columns on `dt`.

### Behavior notes

- Built from declarative inputs via `pipeline_input_to_compute_input` (`InputSpec.keys`, `Required` → `join_type="inner"`).

### Example

```python
ComputeInput(dt=words_dt, join_type="full")
ComputeInput(dt=labels_dt, join_type="inner", keys={"item_id": "id"})
```

---

## `ComputeOutput`

When to use: Describe one output `DataTable` for a runtime step, with optional transform-key → output-key mapping for cleanup.

```python
@dataclass
class ComputeOutput:
    dt: DataTable
    keys: dict[str, str] | None = None
```

### Fields

| Name | Type | Default | Description |
|---|---|---|---|
| `dt` | `DataTable` | — | Output table handle. |
| `keys` | `dict[str, str] \| None` | `None` | Map `{transform_key: output_primary_key}`. Used when storing/deleting output rows for a batch index. |

### `ComputeOutput.primary_keys`

```python
@property
def primary_keys(self) -> list[str]: ...
```

### Returns

Output-side key names: keys of `keys` if set, else `dt.primary_keys`.

### `ComputeOutput.primary_schema`

```python
@property
def primary_schema(self) -> MetaSchema: ...
```

### Returns

SQLAlchemy columns for output-side keys (aliased when `keys` is set).

### Example

```python
ComputeOutput(dt=out_dt)
ComputeOutput(dt=posts_dt, keys={"post_id": "id"})
```

---

## `ComputeStep`

When to use: Base runtime step type. Declarative `PipelineStep.build_compute` produces concrete subclasses (e.g. `BaseBatchTransformStep`).

### Overview

| Member | Type | Description |
|---|---|---|
| `name` | `str` | Unique step identifier. |
| `input_dts` | `list[ComputeInput]` | Normalized input bindings. |
| `output_dts` | `list[ComputeOutput]` | Output bindings. |
| `labels` | `Labels` (property) | Step labels; `[]` when unset. |
| `executor_config` | `ExecutorConfig \| None` | Optional executor resources. |

---

## `ComputeStep.__init__`

```python
def __init__(
    self,
    name: str,
    input_dts: Sequence[ComputeInput],
    output_dts: Sequence[ComputeOutput],
    labels: Labels | None = None,
    executor_config: ExecutorConfig | None = None,
) -> None: ...
```

### Parameters

| Name | Type | Default | Description |
|---|---|---|---|
| `name` | `str` | — | Step name; must be unique across the pipeline. |
| `input_dts` | `Sequence[ComputeInput]` | — | Input table bindings. |
| `output_dts` | `Sequence[ComputeOutput]` | — | Output table bindings. |
| `labels` | `Labels \| None` | `None` | Optional `[(key, value), …]` labels. |
| `executor_config` | `ExecutorConfig \| None` | `None` | Passed to batch executors. |

### Returns

`None`

### Behavior notes

- Stores `input_dts` and `output_dts` as lists.

---

## `ComputeStep.get_name`

When to use: Return the step name (alias for `name`).

```python
def get_name(self) -> str: ...
```

### Returns

`str` — `self.name`.

---

## `ComputeStep.format_io`

When to use: Human-readable input → output table names for logging and tracing.

```python
def format_io(self) -> str: ...
```

### Returns

`str` — e.g. `"['words'] -> ['lengths']"`.

---

## `ComputeStep.labels`

```python
@property
def labels(self) -> Labels: ...
```

### Returns

`Labels` — `self._labels` or `[]` if unset.

---

## `ComputeStep.get_status`

When to use: Report processing backlog for this step.

```python
def get_status(self, ds: DataStore) -> StepStatus: ...
```

### Parameters

| Name | Type | Description |
|---|---|---|
| `ds` | `DataStore` | Active datastore. |

### Returns

`StepStatus`

### Raises

| Exception | When |
|---|---|
| `NotImplementedError` | Base `ComputeStep`; concrete steps implement this. |

---

## `ComputeStep.validate`

When to use: Check that shared primary keys between inputs and outputs have compatible column types. Called from `build_compute`.

```python
def validate(self) -> None: ...
```

### Returns

`None`

### Raises

| Exception | When |
|---|---|
| `ValueError` | A join key present in both input and output intersections has mismatched SQLAlchemy column types. |

### Behavior notes

- Computes intersection of primary keys across inputs, across outputs, then their intersection (join keys).
- No-op when there are no overlapping keys.

---

## `ComputeStep.get_full_process_ids`

When to use: List index batches for a full run (all rows needing processing).

```python
def get_full_process_ids(
    self,
    ds: DataStore,
    chunk_size: int | None = None,
    run_config: RunConfig | None = None,
) -> tuple[int, Iterable[IndexDF]]: ...
```

### Parameters

| Name | Type | Default | Description |
|---|---|---|---|
| `ds` | `DataStore` | — | Active datastore. |
| `chunk_size` | `int \| None` | `None` | Max indexes per yielded batch; step-specific default when `None`. |
| `run_config` | `RunConfig \| None` | `None` | Filters and labels. |

### Returns

`tuple[int, Iterable[IndexDF]]` — `(batch_count_or_index_count, iterator of index DataFrames)`.

### Raises

| Exception | When |
|---|---|
| `NotImplementedError` | Base class. |

---

## `ComputeStep.get_change_list_process_ids`

When to use: List index batches limited to upstream table changes.

```python
def get_change_list_process_ids(
    self,
    ds: DataStore,
    change_list: ChangeList,
    run_config: RunConfig | None = None,
) -> tuple[int, Iterable[IndexDF]]: ...
```

### Parameters

| Name | Type | Description |
|---|---|---|
| `ds` | `DataStore` | Active datastore. |
| `change_list` | `ChangeList` | Per-table changed indexes from prior steps. |
| `run_config` | `RunConfig \| None` | Filters and labels. |

### Returns

`tuple[int, Iterable[IndexDF]]`

### Raises

| Exception | When |
|---|---|
| `NotImplementedError` | Base class. |

---

## `ComputeStep.run_full`

When to use: Process all pending indexes for this step.

```python
def run_full(
    self,
    ds: DataStore,
    run_config: RunConfig | None = None,
    executor: Executor | None = None,
) -> None: ...
```

### Parameters

| Name | Type | Default | Description |
|---|---|---|---|
| `ds` | `DataStore` | — | Active datastore. |
| `run_config` | `RunConfig \| None` | `None` | Run context. |
| `executor` | `Executor \| None` | `None` | Batch executor; step may default to `SingleThreadExecutor`. |

### Returns

`None`

### Raises

| Exception | When |
|---|---|
| `NotImplementedError` | Base class. |

### Behavior notes

- Used by `run_steps` for each step in sequence.
- `run_config.callback` receives step lifecycle events when set.

---

## `ComputeStep.run_changelist`

When to use: Process only indexes affected by the given changelist; return downstream changes.

```python
def run_changelist(
    self,
    ds: DataStore,
    change_list: ChangeList,
    run_config: RunConfig | None = None,
    executor: Executor | None = None,
) -> ChangeList: ...
```

### Parameters

| Name | Type | Default | Description |
|---|---|---|---|
| `ds` | `DataStore` | — | Active datastore. |
| `change_list` | `ChangeList` | — | Upstream changes. |
| `run_config` | `RunConfig \| None` | `None` | Run context. |
| `executor` | `Executor \| None` | `None` | Batch executor. |

### Returns

`ChangeList` — indexes changed on output tables.

### Raises

| Exception | When |
|---|---|
| `NotImplementedError` | Base class. |

---

## `ComputeStep.run_idx`

When to use: Process a single explicit index batch (CLI / debugging).

```python
def run_idx(
    self,
    ds: DataStore,
    idx: IndexDF,
    run_config: RunConfig | None = None,
) -> ChangeList: ...
```

### Parameters

| Name | Type | Description |
|---|---|---|
| `ds` | `DataStore` | Active datastore. |
| `idx` | `IndexDF` | Transform or table index to process. |
| `run_config` | `RunConfig \| None` | Run context. |

### Returns

`ChangeList`

### Raises

| Exception | When |
|---|---|
| `NotImplementedError` | Base class. |

---

## Helper functions

### `pipeline_input_to_compute_input`

```python
def pipeline_input_to_compute_input(
    ds: DataStore,
    catalog: Catalog,
    input: PipelineInput,
) -> ComputeInput: ...
```

### Behavior notes

- Resolves table via catalog.
- `InputSpec` → `keys` from spec; plain input → `keys=None`.
- `Required` → `join_type="inner"`; otherwise `"full"`.

### `pipeline_output_to_compute_output`

```python
def pipeline_output_to_compute_output(
    ds: DataStore,
    catalog: Catalog,
    output: PipelineOutput,
) -> ComputeOutput: ...
```

### Behavior notes

- `OutputSpec` → `keys` from spec.

### `make_mungled_step_name`

```python
def make_mungled_step_name(
    cls,
    base_name: str,
    input_dts: Sequence[ComputeInput],
    output_dts: Sequence[ComputeOutput],
) -> str: ...
```

### Returns

`str` — `{base_name}_{5-char shake_128 hex}` from class name, base name, and I/O table names.

---

## See also

- [Steps index](./steps/index.md) — declarative step → runtime mapping
- [BaseBatchTransformStep](./steps/batch-transform.md#basebatchtransformstep) — main `ComputeStep` implementation
- [Compute step lifecycle](../explanation/compute-step-lifecycle.md)
- [Pipeline / Catalog](./pipeline-catalog.md) — `build_compute`, `run_steps`
