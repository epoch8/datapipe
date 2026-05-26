# Datapipe

[Datapipe](https://datapipe.dev/) is a real-time, incremental ETL library for Python with record-level dependency tracking.

The library is designed for describing data processing pipelines and is capable
of tracking dependencies for each record in the pipeline. This ensures that
tasks within the pipeline receive only the data that has been modified, thereby
improving the overall efficiency of data handling.

https://datapipe.dev/

This repository contains the Datapipe Python packages as a `uv` monorepo workspace.

## Packages

- `libs/datapipe-core` - core incremental ETL library, import package `datapipe`.
- `libs/datapipe-ml` - ML addon for Datapipe, import package `datapipe_ml`.
- `libs/datapipe-label-studio` - Label Studio integration, import package `datapipe_label_studio`.
- `libs/datapipe-cvat` - CVAT integration, import package `datapipe_cvat`.

## Development

The workspace is configured in the root `pyproject.toml`. Package code and
package-local tests stay inside each `libs/*` directory. Shared docs and
examples live at the repository root.

Common commands:

```bash
uv sync --all-packages
uv run pytest libs/datapipe-core/tests
uv run pytest libs/datapipe-ml/tests -m "not training and not slow and not e2e and not tensorflow and not torch"
uv run pytest -vv -x libs/datapipe-label-studio/tests
uv run python -c "import datapipe_cvat.cvat_step, datapipe_cvat.utils"
```

## Documentation

Documentation lives in `libs/datapipe-core/docs`. Design notes live in
`libs/datapipe-core/design-docs`.

## Version Compatibility

At the moment, the datapipe library is under active development. Versions:
`v0.*.*`

It should be expected that each minor version is not backward compatible with
the previous one. That is, `v0.7.0` is not compatible with `v0.6.1`. Dependencies
should be fixed to the exact minor version.

After stabilization and transition to the major version `v1.*.*`, the common
rules will apply: all versions with the same major component are compatible.
