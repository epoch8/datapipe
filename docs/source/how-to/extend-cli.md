# How to Extend the Datapipe CLI

Add custom Click commands to the `datapipe` CLI via Python entry points.

## Goal

Ship project- or package-specific commands (for example start an API server) that share the same loaded pipeline and executor as built-in commands.

## Steps

### 1. Write a register function

Datapipe loads every entry point in group `datapipe.cli` and calls it with the root Click group:

```python
import click

def register_commands(cli: click.Group) -> None:
    @cli.command()
    @click.option("--host", type=click.STRING, default="0.0.0.0")
    @click.option("--port", type=click.INT, default=8000)
    @click.pass_context
    def api(ctx: click.Context, host: str, port: int) -> None:
        pipeline = ctx.obj["pipeline"]
        executor = ctx.obj.get("executor")
        # … start your service using pipeline / executor …
```

Signature: `register_commands(cli: click.Group) -> None`. Add `@cli.command()` / groups as needed.

### 2. Use context objects

After global options resolve, `ctx.obj` includes:

| Key | Type | Meaning |
|---|---|---|
| `pipeline` | `DatapipeApp` (or app wrapper) | Loaded app from `--pipeline` / `app.py` |
| `executor` | `Executor` \| `None` | Executor selected via `--executor` |

Parent params (such as the `--pipeline` spec string) are available on `ctx.parent.params` when you need the import path as well as the instance.

### 3. Declare the entry point

**pyproject.toml** (setuptools / hatch / uv):

```toml
[project.entry-points."datapipe.cli"]
my_project = "my_project.cli:register_commands"
```

Install the package into the same environment as `datapipe-core`. New commands appear under `datapipe --help`.

### 4. Run your command

```bash
datapipe --pipeline my_project.app:app api --port 8000
```

Global flags (`--pipeline`, `--executor`, `--debug`, …) stay on the root command; subcommand options stay on the subcommand.

## Expected result

- `datapipe --help` lists your command next to `run`, `step`, `db`, …
- Your command receives an already-initialized pipeline in `ctx.obj["pipeline"]`.

## Example

`datapipe-app` registers `datapipe api` this way: `datapipe_app.app.cli:register_commands` (`libs/datapipe-app/datapipe_app/app/cli.py`, entry point in that package’s `pyproject.toml`).

## See also

- [CLI Commands](../reference/cli.md)
