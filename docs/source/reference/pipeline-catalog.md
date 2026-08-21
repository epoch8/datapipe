# Pipeline / Catalog / DatapipeApp

Core wiring: declare tables in a catalog, list pipeline steps, build a compute graph, run it.

Module: `datapipe.compute`

---

## `Table`

When to use: Wrap a `TableStore` so the catalog can name and resolve it.

```python
@dataclass
class Table:
    store: TableStore
    name: str | None = None
```

### Arguments

| Arg | Description |
|---|---|
| `store` | Backend that holds row data. |
| `name` | Optional. Required when the `Table` is passed inline as a step input/output instead of via catalog key. |

### Notes

- Catalog keys are the usual names used in steps (`"images"`). Inline `Table` objects must set `name`.
- Passing an SQLAlchemy ORM class as a step I/O implicitly creates a `TableStoreDB` and registers it.

### See also

- [Table / DataTable / DataStore](./table.md)
- [TableStore backends](./stores/index.md)

---

## `Catalog`

When to use: Map table names to stores; resolve names / ORM / `Table` to `DataTable` at build time.

```python
class Catalog:
    def __init__(self, catalog: dict[str, Table]): ...
    def add_datatable(self, name: str, dt: Table) -> None: ...
    def remove_datatable(self, name: str) -> None: ...
    def init_all_tables(self, ds: DataStore) -> None: ...
    def get_datatable(self, ds: DataStore, table: TableOrName) -> DataTable: ...
```

### Arguments

| Arg | Description |
|---|---|
| `catalog` | `dict` of name → `Table`. |

### Notes

- `get_datatable` accepts `str` (catalog key), `Table` (must have `name`), or SQLAlchemy ORM class.
- Re-registering the same name with a different store raises.
- `build_compute` calls `init_all_tables` so every catalog entry gets a `DataTable` before steps run.

### See also

- [Tables and TableStores](../concepts/tables-and-stores.md)

---

## `PipelineStep`

When to use: Base for declarative steps; implement `build_compute` to emit runtime `ComputeStep`s.

```python
class PipelineStep(ABC):
    @abstractmethod
    def build_compute(self, ds: DataStore, catalog: Catalog) -> list[ComputeStep]: ...
```

### See also

- [Steps index](./steps/index.md)

---

## `Pipeline`

When to use: Ordered list of declarative steps that become the compute graph.

```python
@dataclass
class Pipeline:
    steps: Sequence[PipelineStep]
```

### Notes

- Order is execution order for full runs.
- Duplicate compute-step names after build raise.

---

## `DatapipeApp`

When to use: Bundle datastore, catalog, and pipeline for CLI and programmatic runs. Builds compute steps on construction.

```python
class DatapipeApp:
    def __init__(self, ds: DataStore, catalog: Catalog, pipeline: Pipeline): ...
    # attrs: ds, catalog, pipeline, steps: list[ComputeStep]
```

### Notes

- `self.steps = build_compute(ds, catalog, pipeline)` at init.
- CLI loads this object via `--pipeline module:app`.

### See also

- [CLI](./cli.md)

---

## `build_compute`

When to use: Expand a `Pipeline` into validated `ComputeStep`s without running them.

```python
def build_compute(
    ds: DataStore,
    catalog: Catalog,
    pipeline: Pipeline,
) -> list[ComputeStep]: ...
```

### Notes

- Initializes all catalog tables, concatenates each step’s `build_compute` result, rejects duplicate names, calls `validate()` on each step.

---

## `run_pipeline`

When to use: Build and run the full pipeline once (all indexes that need work).

```python
def run_pipeline(
    ds: DataStore,
    catalog: Catalog,
    pipeline: Pipeline,
    run_config: RunConfig | None = None,
) -> None: ...
```

### Notes

- Equivalent to `run_steps(ds, build_compute(...), run_config)`.
- Does not take an `executor`; use `run_steps` if you need a custom executor.

---

## `run_steps`

When to use: Run a prepared list of `ComputeStep`s in order (`run_full` each).

```python
def run_steps(
    ds: DataStore,
    steps: Sequence[ComputeStep],
    run_config: RunConfig | None = None,
    executor: Executor | None = None,
) -> None: ...
```

### Notes

- Honors cancel tokens and `RunConfig.callback` (`on_run_*` / `on_step_*`).
- Default executor behavior is per-step (batch transforms default to `SingleThreadExecutor` if `executor` is `None`).

---

## `run_changelist`

When to use: Build the pipeline and process only indexes covered by a `ChangeList`.

```python
def run_changelist(
    ds: DataStore,
    catalog: Catalog,
    pipeline: Pipeline,
    changelist: ChangeList,
    run_config: RunConfig | None = None,
) -> None: ...
```

### Notes

- Delegates to `run_steps_changelist`.

---

## `run_steps_changelist`

When to use: Propagate a changelist through batch-transform steps until empty (or 100 iterations).

```python
def run_steps_changelist(
    ds: DataStore,
    steps: list[ComputeStep],
    changelist: ChangeList,
    run_config: RunConfig | None = None,
    executor: Executor | None = None,
) -> None: ...
```

### Notes

- Only `BaseBatchTransformStep` subclasses participate; other step types are skipped in the loop.
- Each iteration feeds the previous iteration’s output changes into the next.

### See also

- [ChangeList](./types.md#changelist)
- [Incremental processing](../concepts/incremental-processing.md)
- [Compute step lifecycle](../explanation/compute-step-lifecycle.md)
