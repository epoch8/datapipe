# How to Run Steps with RayExecutor

Parallelize batch chunk processing with Ray instead of the default single-threaded executor.

## Goal

Speed up large dirty sets by running `process_fn` chunks as Ray remote tasks.

## When to use what

| Executor | Use when |
|---|---|
| `SingleThreadExecutor` (default) | Local debug, small batches, steps that must stay in-process |
| `RayExecutor` | Many independent chunks; you want CPU/GPU/memory hints per step |

`SingleThreadExecutor` ignores `executor_config` resource fields. Ray honors them.

## Steps

### 1. Install the Ray extra

```bash
pip install "datapipe-core[ray]"
```

That installs `ray[default]`. Without it, `datapipe --executor RayExecutor` fails on import.

### 2. Run via CLI

```bash
datapipe --executor RayExecutor run
datapipe --executor RayExecutor step --name=my_step run
```

The CLI calls `ray.init()` before building the executor and sets `RAY_ENABLE_UV_RUN_RUNTIME_ENV=0` if unset (so Ray does not fight `uv run` env injection).

### 3. Attach `executor_config` on batch steps

```python
from datapipe.executor import ExecutorConfig
from datapipe.step.batch_transform import BatchTransform

BatchTransform(
    my_func,
    inputs=["a"],
    outputs=["b"],
    executor_config=ExecutorConfig(
        cpu=2,
        memory=4 * 1024**3,  # bytes
        parallelism=50,       # max in-flight Ray tasks
    ),
)
```

If `executor_config` is omitted, Ray caps outstanding futures at **10**. With a config object, default `parallelism` is **100**.

## Pitfalls

- **`ray.init` via CLI** — `--executor RayExecutor` always initializes Ray in the CLI process. For a pre-started cluster, point Ray at it with the usual env / `ray.init` address conventions before or instead of relying on a bare local init; the CLI itself does not take a cluster URL flag.
- **Local vs cluster** — default `ray.init()` is a local runtime on the machine running the CLI. Scaling out means a Ray cluster that workers can reach, plus that workers can import your pipeline code and open the same DBs / stores.
- **Callbacks** — progress callbacks stay on the driver; remotes strip `RunConfig.callback`.
- **Pickling** — remote tasks ship `DataStore`, indexes, and your `process_fn` path; stores and closures must be Ray/cloudpickle-friendly.

## Expected result

Dirty keys still come from incremental meta. Ray only parallelizes **how** chunks of those keys are processed.

## See also

- [Executors reference](../reference/executors.md)
- [BatchTransform](../reference/steps/batch-transform.md)
- [CLI `--executor`](../reference/cli.md)
