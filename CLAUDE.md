# datapipe

This is a `uv` workspace (`pyproject.toml` + `uv.lock`) with member libs under `libs/*`.

## Commands

- Use `uv sync --all-packages --all-extras` to install/sync the environment — this is a multi-package workspace, so `--all-packages` is what installs every workspace member together.
