# Extending `datapipe` cli

> **Needs review.** This page was carried over from the previous documentation and has not been updated yet.

## Entry point

Datapipe offers a way to add additional cli commands. It is achieved by
utilizing Python entrypoints mechanism.

Datapipe looks for entrypoints with group name `datapipe.cli` and expects a
function with signature:

```python
import click

def register_commands(cli: click.Group) -> None:
    ...
```

## Context

Plugin can expect some information in `click.Context`:

* `ctx.obj["pipeline"]`: `datapipe.compute.DatapipeApp` instance of DatapipeApp
  with all necessary initialization steps performed

* `ctx.obj["executor"]`: `datapipe.executor.Executor` contains an instance of
  Executor which will be used to perform computation

## Example

To see example of extending `datapipe` cli see `datapipe_app.cli`:
[https://github.com/epoch8/datapipe-app/blob/master/datapipe_app/cli.py](https://github.com/epoch8/datapipe-app/blob/master/datapipe_app/cli.py)

## Run Callbacks

`datapipe run` and `datapipe step run` attach a
[`RunCallback`](../reference/run-callbacks.md) to every run for lifecycle
events and step progress. In addition to the built-in progress printer, other
packages can plug in their own callback (e.g. recording runs to an Ops
dashboard) via the `datapipe.run_callbacks` entry-point group:

```python
from datapipe.compute import ComputeStep, DatapipeApp
from datapipe.run_callback import RunCallback
from datapipe.types import Labels

def make_run_callback(
    app: DatapipeApp,
    steps: list[ComputeStep],
    *,
    labels: Labels,
    pipeline_spec: str | None,
) -> RunCallback | None:
    ...
```

Register it the same way as a `datapipe.cli` entry point, under the
`datapipe.run_callbacks` group instead:

```toml
[project.entry-points."datapipe.run_callbacks"]
my_callback = "my_package.callbacks:make_run_callback"
```

Return `None` from the factory to opt the run out without registering a
callback. Every entry point in this group is loaded and combined via
`CompositeRunCallback`, so a failure in one callback is logged rather than
aborting the run or the other callbacks. Pass `--no-callbacks` to `datapipe
run` / `datapipe step run` to skip loading entry-point callbacks for a single
invocation (the built-in progress printer is unaffected — see
[CLI Commands](../reference/cli.md)).
