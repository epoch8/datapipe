# datapipe-ui

React Ops SPA (`@datapipe/ui`) + Python package that ships built static files
(`datapipe.ui_static` entry point).

Cloud Ops dashboard against slim **API v1alpha3**: pipeline graph, tables, transforms,
interactive transform runs. Install alongside `datapipe-app[ui]`.

## Quick start

```bash
# from monorepo root (datapipe/)
make -C libs/datapipe-ui install
make -C libs/datapipe-ui start      # dev server (proxy API separately)
make -C libs/datapipe-ui build-package
make -C libs/datapipe-ui test
```

```bash
uv pip install -e "libs/datapipe-app[ui]"
```

## Layout

| Path | Role |
|------|------|
| `src/` | Ops React app |
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
