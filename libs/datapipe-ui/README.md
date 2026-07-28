# datapipe-ui

React Ops SPA (`@datapipe/ui`) + Python package that ships built static files (`datapipe.ui_static` entry point).

Core-only dashboard: pipeline overview, graph, runs, tables, transforms. Optional UI plugins extend routes, navigation, and API via `registerUiPlugin()`.

## Quick start

```bash
# from monorepo root (datapipe/)
make -C libs/datapipe-ui install
make -C libs/datapipe-ui start      # dev server
make -C libs/datapipe-ui build      # production build
make -C libs/datapipe-ui test
```

Python (serves built static when installed):

```bash
uv pip install -e "libs/datapipe-app[ui]"
```

## Layout

| Path | Role |
|------|------|
| `src/` | Core Ops React app |
| `src/plugins/` | Generic plugin registry (`registerUiPlugin`, route/nav/API aggregation) |
| `datapipe_ui/static/` | Built SPA copied here for the Python wheel |
| `scripts/copy-static.sh` | `build/` → `datapipe_ui/static/` |

## Commands

```bash
make install
make start
make build
make build-package   # build + copy static for Python packaging
make test
make package         # uv wheel (runs build-package first)
```

## Tests

```bash
yarn workspace @datapipe/ui test
```

Jest covers core API helpers (`src/api/http.test.ts`). Plugin packages add their own tests and production build checks.
