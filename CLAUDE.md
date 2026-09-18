# datapipe

This is a `uv` workspace (`pyproject.toml` + `uv.lock`) with member libs under `libs/*`,
and a `pnpm` workspace (`pnpm-workspace.yaml` + `pnpm-lock.yaml`) with the JS packages
under `packages/*` and `libs/datapipe-ui`.

## Commands

- Use `uv sync --all-packages --all-extras` to install/sync the environment — this is a multi-package workspace, so `--all-packages` is what installs every workspace member together.
- Use `pnpm install` for the JS workspace, and `pnpm --filter @datapipe/ui <script>` to run the UI's scripts.

## Which workspace is used

uv and pnpm both use the nearest workspace root above the current directory. So
any command run in or below this directory uses this directory's `.venv`,
`uv.lock` and `pnpm-lock.yaml`, even when this directory is checked out inside a
larger workspace.

## Code style

- Prefer modern `X | None` union syntax over `Optional[X]` in new or touched code.
