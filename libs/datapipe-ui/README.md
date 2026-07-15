# Datapipe Ops UI (`@datapipe/ui`)

React SPA for the Datapipe Ops dashboard. ML-specific pages live in the sibling package [`@datapipe/ui-ml`](../datapipe-ui-ml/) and are compiled into the same bundle via a plugin registry.

## Packages

| Package | Role |
|---------|------|
| `libs/datapipe-ui` | Core Ops UI (graph, runs, pipelines) + CRA host |
| `libs/datapipe-ui-ml` | ML plugin (metrics, training, images, ops-specs pages) |
| `datapipe-ui` (Python) | Ships built static files; registered via `datapipe.ui_static` |
| `datapipe-app` | FastAPI API; serves UI when `datapipe-ui` is installed |

## Development

From the monorepo root:

```bash
yarn install
yarn workspace @datapipe/ui start
```

Build with ML plugin (default):

```bash
yarn workspace @datapipe/ui build
```

Core-only build (stubs out `@datapipe/ui-ml`):

```bash
yarn workspace @datapipe/ui build:core
```

Copy build output into the Python wheel tree:

```bash
yarn workspace @datapipe/ui build:package
```

## Python install

```bash
uv pip install -e "libs/datapipe-ui"
uv pip install -e "libs/datapipe-app[ui]"
```

Or install ML backend + UI together:

```bash
uv pip install -e "libs/datapipe-app[ml]"
```

The API serves the SPA at `/` when static assets are present.

## Tests

Jest runs from the UI host (`@datapipe/ui`). ML plugin tests import `@datapipe/ui-ml` sources via webpack/jest aliases.

| Location | What |
|----------|------|
| `libs/datapipe-ui/src/api/http.test.ts` | Core HTTP client |
| `libs/datapipe-ui/src/api/mlOps.filters.test.ts` | ML API query serialization (`@datapipe/ui-ml`) |
| `libs/datapipe-ui/src/features/ops/shared/tableFilters.test.ts` | Ops table filters (`@datapipe/ui-ml`) |
| `libs/datapipe-ui/src/features/ops/shared/opsTableSort.test.ts` | Ops table sort (`@datapipe/ui-ml`) |
| `libs/datapipe-ui/src/ml/recordFields.test.ts` | ML record field ordering (`@datapipe/ui-ml`) |
| `libs/datapipe-ui/tests/test_static.py` | Python static dir resolution |

```bash
yarn workspace @datapipe/ui test
pytest libs/datapipe-ui/tests
```

CI: `.github/workflows/lib-datapipe-ui.yml`
