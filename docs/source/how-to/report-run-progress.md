# How to Report Progress from a Pipeline Run

`run_steps` reports lifecycle events and step progress through a single
`RunCallback` attached to `RunConfig.callback`. See
[Run Callbacks](../reference/run-callbacks.md) for the full API reference.

## Use the built-in progress printer

If you call `run_steps` directly (outside of the `datapipe` CLI, which
already attaches one for you), attach `StdoutRunCallback` to get throttled
`step: completed/total` lines with average time per item and ETA:

```python
from datapipe.compute import run_steps
from datapipe.run_config import RunConfig
from datapipe.run_callback_stdout import StdoutRunCallback

run_steps(
    ds,
    steps,
    run_config=RunConfig(callback=StdoutRunCallback()),
)
```

## Write a custom callback

`RunCallback` is a `typing.Protocol` — any object with the matching methods
works, no base class required. Implement only the events you care about; for
example, forwarding progress to your own metrics system:

```python
class MetricsRunCallback:
    def on_run_start(self, steps): pass
    def on_step_start(self, step): pass

    def on_step_progress(self, step, completed, total):
        my_metrics.gauge("datapipe.step.progress", completed, tags={"step": step.name})

    def on_step_success(self, step): pass
    def on_step_error(self, step, error): pass
    def on_run_success(self): pass
    def on_run_error(self, error): pass


run_steps(ds, steps, run_config=RunConfig(callback=MetricsRunCallback()))
```

A callback that raises inside `run_steps` will propagate and abort the run —
`RunConfig.callback` itself does not catch exceptions. Use
`CompositeRunCallback` (next section) if you want failures in one callback
isolated from the others and from the pipeline.

## Combine multiple callbacks

```python
from datapipe.run_callback import CompositeRunCallback
from datapipe.run_callback_stdout import StdoutRunCallback

run_config = RunConfig(
    callback=CompositeRunCallback([StdoutRunCallback(), MetricsRunCallback()]),
)
run_steps(ds, steps, run_config=run_config)
```

`CompositeRunCallback` calls each sub-callback's method inside its own
`try`/`except`, logging and continuing on failure — this is the fail-open
behavior described in the reference page.

## Attach a callback to the `datapipe` CLI

To have your callback attached automatically by `datapipe run` / `datapipe
step run` (e.g. for an Ops dashboard, without changing pipeline code),
register a `datapipe.run_callbacks` entry point — see
[Extend the CLI](./extend-cli.md#run-callbacks) for the factory signature.
Users can skip all entry-point callbacks for a single invocation with
`datapipe run --no-callbacks` (the built-in `StdoutRunCallback` still runs;
`--no-callbacks` only controls entry-point-loaded callbacks).
