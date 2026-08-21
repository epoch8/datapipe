# Executors

How batch indexes are processed: sequentially or via Ray workers.

Modules: `datapipe.executor`, `datapipe.executor.ray`

---

## `ExecutorConfig`

When to use: Attach resource / parallelism hints to a step (honored by `RayExecutor`).

```python
@dataclass
class ExecutorConfig:
    memory: int | None = None
    cpu: float | None = None
    gpu: int | None = None
    parallelism: int = 100
```

### Arguments

| Arg | Description |
|---|---|
| `memory` | Ray remote `memory` (bytes), if set. |
| `cpu` | Ray remote `num_cpus`. |
| `gpu` | Ray remote `num_gpus`. |
| `parallelism` | Max in-flight Ray tasks before waiting (`RayExecutor`). Default `100` on the config object; if no config is passed, Ray uses `10`. |

### Notes

- Pass via `BatchTransform(..., executor_config=...)` (and related batch steps).
- `SingleThreadExecutor` ignores resource fields.

---

## `Executor`

When to use: Abstract runner for `process_fn` over an index generator.

```python
class Executor(ABC):
    @abstractmethod
    def run_process_batch(
        self,
        step: ComputeStep,
        ds: DataStore,
        idx_count: int,
        idx_gen: Generator[IndexDF, None, None],
        process_fn: ProcessFn,
        run_config: RunConfig | None = None,
        executor_config: ExecutorConfig | None = None,
    ) -> ChangeList: ...
```

### Notes

- `ProcessFn`: `(ds, idx, run_config=None) -> ChangeList`.
- Batch transform steps call this from `run_full` / `run_changelist`.

---

## `SingleThreadExecutor`

When to use: Default local execution — process one index chunk at a time in the current process.

```python
class SingleThreadExecutor(Executor):
    def run_process_batch(...) -> ChangeList: ...
```

### Notes

- Reports progress via `run_config.callback.on_step_progress`.
- Checks cancel tokens between chunks; always closes `idx_gen`.

---

## `RayExecutor`

When to use: Parallelize batch processing across a Ray cluster / local Ray runtime.

```python
class RayExecutor(Executor):
    def run_process_batch(...) -> ChangeList: ...
```

### Notes

- Wraps `process_fn` in `@ray.remote` using `ExecutorConfig` resources when provided.
- Strips callbacks from the remote `RunConfig` (progress stays on the driver).
- Caps outstanding futures with `parallelism` (config default `100`, or `10` if `executor_config` is `None`).
- On cancel or error, cancels outstanding futures.
- CLI: `datapipe --executor RayExecutor ...` calls `ray.init()` and sets `RAY_ENABLE_UV_RUN_RUNTIME_ENV=0` if unset.

### See also

- [BatchTransform](./steps/batch-transform.md)
- [CLI `--executor`](./cli.md)
