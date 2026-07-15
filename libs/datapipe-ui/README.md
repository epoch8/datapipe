# datapipe-ui

React Ops SPA (`@datapipe/ui`) + Python package that ships built static files (`datapipe.ui_static` entry point). ML pages come from the sibling plugin [`datapipe-ui-ml`](../datapipe-ui-ml/).

## Quick start

```bash
make install    # yarn install (monorepo root)
make start      # dev server
make build-package   # production build → datapipe_ui/static/
make test
make package    # build-package + uv build
```

Docker build (Node 18, no local Node required): `make build-docker`.

## Python install

```bash
make build-package
uv pip install -e "libs/datapipe-ui"
uv pip install -e "libs/datapipe-app[ui]"   # or [ml] for ML plugin + backend
```

The API serves the SPA at `/` when `datapipe-ui` is installed.

## Packages

| Package | Role |
|---------|------|
| `libs/datapipe-ui` | Core Ops UI + CRA host |
| `libs/datapipe-ui-ml` | ML plugin (routes, nav, API client) |
| `datapipe-app` | FastAPI; mounts static assets |

Core-only build (stubs ML plugin): `make build-core`.

## Tests

Jest runs from this package; ML tests import `@datapipe/ui-ml` via aliases.

```bash
make test
pytest libs/datapipe-ui/tests
```

CI: `.github/workflows/lib-datapipe-ui.yml`
