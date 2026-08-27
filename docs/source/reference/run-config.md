# RunConfig, RunCallback, and cancellation

Run-scoped configuration, lifecycle hooks, and cooperative cancellation for pipeline execution.

Modules: `datapipe.run_config`, `datapipe.run_callback`, `datapipe.cancel`

---

## `LabelDict`

Type alias: `dict[str, Any]` — string keys with arbitrary values used for filters and run labels.

---

## `RunConfig`

When to use: Pass filters, labels, failure mode, and callbacks into `run_full`, `run_changelist`, `run_steps`, and table/meta operations.

```python
@dataclass
class RunConfig:
    filters: LabelDict = field(default_factory=dict)
    labels: LabelDict = field(default_factory=dict)
    fail_fast: bool = field(default_factory=lambda: settings.fail_fast)
    callback: RunCallback | None = None
```

### Fields

| Name | Type | Default | Description |
|---|---|---|---|
| `filters` | `LabelDict` | `{}` | Global row filters. Only rows whose table has a matching column/key are processed. Merged with step-level filters on batch transforms. |
| `labels` | `LabelDict` | `{}` | Arbitrary labels attached to the run (logging, callbacks). Steps add `step_name` via `RunConfig.add_labels`. |
| `fail_fast` | `bool` | `settings.fail_fast` | When `True`, first batch exception propagates and stops the run. When `False`, batch steps log/mark error and continue. |
| `callback` | `RunCallback \| None` | `None` | Lifecycle and progress hooks. Use `CompositeRunCallback` for multiple listeners. |

---

## `RunConfig.add_labels`

When to use: Immutably merge labels into an existing or new config.

```python
@classmethod
def add_labels(cls, rc: RunConfig | None, labels: LabelDict) -> RunConfig: ...
```

### Parameters

| Name | Type | Description |
|---|---|---|
| `rc` | `RunConfig \| None` | Base config; `None` creates a new config with only `labels`. |
| `labels` | `LabelDict` | Labels to merge (overwrites keys on conflict). |

### Returns

`RunConfig` — new instance via `dataclasses.replace`.

### Example

```python
rc = RunConfig.add_labels(None, {"step_name": "count_chars"})
```

---

## `RunConfig.with_callback`

When to use: Attach or replace the run callback.

```python
@classmethod
def with_callback(cls, rc: RunConfig | None, callback: RunCallback) -> RunConfig: ...
```

### Parameters

| Name | Type | Description |
|---|---|---|
| `rc` | `RunConfig \| None` | Base config. |
| `callback` | `RunCallback` | Callback implementation. |

### Returns

`RunConfig`

### Example

```python
from datapipe.run_callback import CompositeRunCallback

rc = RunConfig.with_callback(
    RunConfig(filters={"project": "demo"}),
    CompositeRunCallback([my_callback, metrics_callback]),
)
```

---

## `RunCallback`

When to use: Observe pipeline and step lifecycle without replacing the runner. Structural protocol — any object with these methods qualifies.

```python
class RunCallback(Protocol):
    def on_run_start(self, steps: Sequence[ComputeStep]) -> None: ...
    def on_step_start(self, step: ComputeStep) -> None: ...
    def on_step_progress(
        self,
        step: ComputeStep,
        completed: int,
        total: int | None,
    ) -> None: ...
    def on_step_success(self, step: ComputeStep) -> None: ...
    def on_step_error(self, step: ComputeStep, error: BaseException) -> None: ...
    def on_run_success(self) -> None: ...
    def on_run_error(self, error: BaseException) -> None: ...
```

### Method contracts

| Method | When invoked | Notes |
|---|---|---|
| `on_run_start` | Start of `run_steps` | Receives full step list. |
| `on_step_start` | Before each step in `run_steps` | Not emitted by `run_steps_changelist` loop today. |
| `on_step_progress` | During `fill_metadata` and executor batch loops | `completed` / `total` batch counts. |
| `on_step_success` | Step finished without exception in `run_steps` | |
| `on_step_error` | Step raised in `run_steps` | Exception re-raised after callback. |
| `on_run_success` | All steps completed in `run_steps` | |
| `on_run_error` | Any uncaught exception in the run loop | |

### Behavior notes

- Callbacks must not replace core execution logic.
- Implementations should be fast; heavy work belongs in background workers fed by callbacks.

---

## `CompositeRunCallback`

When to use: Fan out events to multiple `RunCallback` instances.

```python
@dataclass
class CompositeRunCallback:
    callbacks: Sequence[RunCallback]
```

Each protocol method iterates `callbacks` in order.

### Behavior notes

- **Fail-open**: an exception in one callback is logged (`logger.exception`) and does not block other callbacks or mask pipeline errors.

### Example

```python
CompositeRunCallback([StdoutRunCallback(), db_run_logger])
```

---

## `CancelToken`

When to use: Thread-safe urgent stop for in-process runs. Installed via `cancel_token_scope` for the duration of a background run.

```python
class CancelToken:
    def __init__(self) -> None: ...
```

---

## `CancelToken.is_cancelled`

```python
def is_cancelled(self) -> bool: ...
```

### Returns

`bool` — `True` after `request_cancel`.

---

## `CancelToken.register_process`

When to use: Register a child `multiprocessing` process to terminate on cancel.

```python
def register_process(self, process: BaseProcess) -> None: ...
```

### Behavior notes

- If already cancelled, terminates the process immediately instead of registering.

---

## `CancelToken.unregister_process`

```python
def unregister_process(self, process: BaseProcess) -> None: ...
```

### Behavior notes

- Removes the process from the internal list when it exits normally.

---

## `CancelToken.request_cancel`

When to use: Signal cancellation and terminate all registered child processes.

```python
def request_cancel(self) -> None: ...
```

### Behavior notes

- Sets an internal event, then `terminate()` / `kill()` registered processes (best-effort, 5s + 2s join timeouts).

---

## `CancelToken.raise_if_cancelled`

When to use: Poll cooperative cancellation points (e.g. between steps in `run_steps`).

```python
def raise_if_cancelled(self) -> None: ...
```

### Raises

| Exception | When |
|---|---|
| `RunCancelledError` | Cancel was requested. |

---

## `RunCancelledError`

When to use: Exception raised when a run is stopped by user/API cancel.

```python
class RunCancelledError(KeyboardInterrupt):
    ...
```

### Behavior notes

- Subclasses `KeyboardInterrupt` so training/subprocess interrupt handlers treat it as a user stop.
- Message: `"Run stopped by user"`.

---

## `get_cancel_token`

When to use: Read the current context-var token from worker or step code.

```python
def get_cancel_token() -> CancelToken | None: ...
```

### Returns

`CancelToken | None` — active token when inside `cancel_token_scope`, else `None`.

---

## `cancel_token_scope`

When to use: Context manager that binds a token for the current async/thread context.

```python
@contextmanager
def cancel_token_scope(
    token: CancelToken | None,
) -> Generator[CancelToken | None, None, None]: ...
```

### Parameters

| Name | Type | Description |
|---|---|---|
| `token` | `CancelToken \| None` | Token to install; `None` yields without setting context. |

### Yields

The same `token` (or `None`).

### Behavior notes

- Uses `contextvars.ContextVar`; nested scopes restore the previous token on exit.

### Example

```python
token = CancelToken()
with cancel_token_scope(token):
    run_steps(ds, steps, run_config=RunConfig())
# elsewhere: token.request_cancel()
```

---

## Integration summary

| Component | Uses `RunConfig` | Uses callback | Uses cancel token |
|---|---|---|---|
| `run_steps` | yes | `on_run_*`, `on_step_*` | `get_cancel_token().raise_if_cancelled()` per step |
| `BaseBatchTransformStep.run_full` | yes | via executor progress | executor-dependent |
| `BaseBatchTransformStep.fill_metadata` | yes | `on_step_progress` | — |
| `DataTable.store_chunk` / deletes | optional | — | — |
| `TableMeta.get_stale_idx` | optional filters | — | — |

---

## See also

- [Filter by labels](../how-to/filter-by-labels.md)
- [Observability and run logs](../ops/observability.md)
- [ComputeStep](./compute-step.md)
- [Executors](./executors.md)
