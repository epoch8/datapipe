# Extending `datapipe` cli

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

To see example of extending `datapipe` cli see
[`datapipe_app.cli`](https://github.com/epoch8/datapipe-app/blob/master/datapipe_app/cli.py)
