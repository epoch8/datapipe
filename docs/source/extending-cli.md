# Extending `datapipe` cli

Datapipe offers a way to add additional cli commands. It is achieved by
utilizing Python entrypoints mechanism.

Datapipe looks for entrypoints with group name `datapipe.cli` and expects a
function with signature:

```python
import click

def register_commands(cli: click.Group) -> None:
    ...
```

To see example of extending `datapipe` cli see
[`datapipe-app`](https://github.com/epoch8/datapipe-app)
