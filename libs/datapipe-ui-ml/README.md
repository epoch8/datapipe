# datapipe-ui-ml

ML Ops UI plugin (`@datapipe/ui-ml`) for the Datapipe dashboard: metrics, models, frozen datasets, class metrics, training runs, image browsers, ops-spec pages.

Standalone CRA host that composes the core [`datapipe-ui`](../datapipe-ui/) shell and registers its plugin via `registerUiPlugin()`. Python package `datapipe-ui-ml` ships the built SPA through the `datapipe.ui_static` entry point (preferred over core when both are installed).

## Quick start

```bash
# from monorepo root (datapipe/)
make -C libs/datapipe-ui-ml install
make -C libs/datapipe-ui-ml start      # dev server (core + ML plugin)
make -C libs/datapipe-ui-ml build      # production build
make -C libs/datapipe-ui-ml test
make -C libs/datapipe-ui-ml test-build # production build smoke check
```

Python:

```bash
uv pip install -e "libs/datapipe-app[ml]"
```

## Layout

```
src/
  index.tsx             # SPA entry (imports core App + pluginBootstrap)
  pluginBootstrap.ts    # registerUiPlugin(...)
  api/                  # ML REST client merged into opsApi
  routes.tsx            # ML React Router routes
  nav.tsx               # Sidebar sections
  plugins/PluginSection.tsx
  types/                # opsMl.ts, opsSpecs.ts
  features/ops/         # metrics, models, frozen-datasets, images, training, specs
public/                 # CRA template
datapipe_ui_ml/static/  # built SPA for the Python wheel
```

## Tests

| Command | What |
|---------|------|
| `make test` | Jest unit tests in this package |
| `make test-build` | Production build + bundle smoke assertions |

```bash
yarn workspace @datapipe/ui-ml test
yarn workspace @datapipe/ui-ml test:build
```

Core-only tests live in [`datapipe-ui`](../datapipe-ui/).
