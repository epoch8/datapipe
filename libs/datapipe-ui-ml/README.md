# Datapipe Ops ML UI (`@datapipe/ui-ml`)

TypeScript plugin for ML observability pages in the Datapipe Ops dashboard:

- Metrics, training runs, class metrics
- Image / frozen-dataset record browsers
- Ops-spec driven tables and detail pages

This package is **not** a standalone SPA. It is compiled into [`@datapipe/ui`](../datapipe-ui/) via webpack aliases and registered at runtime through `registerUiMlPlugin()`.

## Python marker package

`datapipe-ui-ml` on PyPI is a lightweight marker package. Install `datapipe-app[ml]` to get the API extensions; the UI itself ships in `datapipe-ui`.

## Layout

```
src/
  api/mlOps.ts       # ML REST client methods (merged into opsApi)
  routes.tsx         # ML React Router routes
  nav.tsx            # Sidebar sections for ops-specs
  features/ops/      # Page components
  types/             # ML + ops-spec TypeScript types
```

## Tests

There is no separate Jest project. Tests for ML UI logic live in `libs/datapipe-ui/src/` and import this package via `@datapipe/ui-ml/...` aliases (see `libs/datapipe-ui/README.md`).

```bash
yarn workspace @datapipe/ui test
```

CI: `.github/workflows/lib-datapipe-ui.yml`
