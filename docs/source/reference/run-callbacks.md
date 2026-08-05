# Run Callbacks

`RunCallback` is the mechanism for observing a `run_steps` execution: run/step
lifecycle events and step progress. It replaces ad-hoc parameters (like a
one-off `progress` callback) with a single, composable interface threaded
through `RunConfig`.

## `RunCallback` protocol

`datapipe.run_callback.RunCallback` — a `typing.Protocol`, so any object with
these methods satisfies it (no inheritance required):

| Method | Fires when |
|---|---|
| `on_run_start(steps: Sequence[ComputeStep])` | Before the first step of a `run_steps` call. |
| `on_step_start(step: ComputeStep)` | Before a step starts executing. |
| `on_step_progress(step: ComputeStep, completed: int, total: int \| None)` | As a step makes progress. `total` is `None` when the amount of work isn't known ahead of time (e.g. `update_external_table`, which iterates a generator of unknown length). |
| `on_step_success(step: ComputeStep)` | After a step completes without raising. |
| `on_step_error(step: ComputeStep, error: BaseException)` | After a step raises. The error is re-raised afterward — this is a notification, not a handler. |
| `on_run_success()` | After all steps complete without raising. |
| `on_run_error(error: BaseException)` | After any step raises and the run aborts. |

## `RunConfig.callback`

```python
class RunConfig:
    ...
    callback: RunCallback | None = None

    @classmethod
    def with_callback(cls, rc: "RunConfig | None", callback: "RunCallback") -> "RunConfig": ...
```

`RunConfig` carries at most one `RunCallback`. `run_steps` reads
`run_config.callback` directly and calls its methods around each step; use
`CompositeRunCallback` to attach more than one.

## `CompositeRunCallback`

```python
from datapipe.run_callback import CompositeRunCallback

callback = CompositeRunCallback([callback_a, callback_b])
```

Fans a single `RunCallback` call out to a list of them. Each sub-callback's
method is called inside its own `try`/`except`: a callback that raises is
logged (`logger.exception`) and does not stop the other callbacks or mask the
pipeline's own error. This is the only place fail-open behavior is
implemented — a lone `RunCallback` assigned directly to `RunConfig.callback`
is not wrapped and will propagate its own exceptions.

## `StdoutRunCallback`

```python
from datapipe.run_callback_stdout import StdoutRunCallback

callback = StdoutRunCallback(min_interval=5.0)
```

Built-in, dependency-free `RunCallback` that prints throttled progress lines
to stdout, e.g.:

```
my_step: 120/500 (avg 0.08s/it, ETA 30.40s)
```

- Printing per step is throttled to at most once every `min_interval`
  seconds, except the very first (`completed == 0`) and last
  (`completed == total`) update for a step, which always print.
- `avg`/`ETA` are derived from the time elapsed since `on_step_start` for
  that step; `ETA` is omitted when `total` is `None`.
- `on_step_start` resets any leftover throttle state for the step, and
  `on_step_success` / `on_step_error` clear it, so per-step state does not
  leak across steps that share a name (e.g. across `--loop` iterations).

The `datapipe` CLI attaches a `StdoutRunCallback` by default to `run`,
`step run`, `step run-changelist`, and `step fill-metadata` — see
[CLI Commands](./cli.md) and [Extend the CLI](../how-to/extend-cli.md).
