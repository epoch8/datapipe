# datapipe-app-ml-ops

Python backend for ML observability in the Ops dashboard: metrics, training runs, frozen datasets, class metrics, ops-spec tables, and image record browsers.

Registered via entry points (`datapipe.observability`, `datapipe.v1alpha3_extensions`, …). Installed automatically with `datapipe-app[ml]`. UI for these endpoints lives in [`datapipe-ui-ml`](../datapipe-ui-ml/).

## Install

```bash
uv sync --package datapipe-app-ml-ops --group dev
```

Or as part of the ML stack:

```bash
uv pip install -e "libs/datapipe-app[ml]"
```

Requires `datapipe-ml` observability tables when metrics/training data is used.

## Tests

```bash
uv run --package datapipe-app-ml-ops pytest libs/datapipe-app-ml-ops/tests -q
```

Lint:

```bash
uv run --package datapipe-app-ml-ops mypy -p datapipe_app_ml_ops \
  --ignore-missing-imports --follow-imports=silent --namespace-packages --check-untyped-defs
```

CI: `.github/workflows/lib-datapipe-app-ml-ops.yml`

## Layout

```
datapipe_app_ml_ops/
  observability/   # routes, metrics/training services, DB models
  ops/             # ops-spec registry, image records, validation
  viz/             # image visualization helpers
```
