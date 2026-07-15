# datapipe-ui-ml

TypeScript ML plugin (`@datapipe/ui-ml`) for the Ops dashboard: metrics, models, frozen datasets, class metrics, training runs, image browsers, ops-spec pages.

Not a standalone SPA — compiled into [`datapipe-ui`](../datapipe-ui/) via webpack aliases and `registerUiMlPlugin()`. The PyPI package `datapipe-ui-ml` is a marker; install with `datapipe-app[ml]`.

## Commands

```bash
make test       # Jest via @datapipe/ui host
make build      # full SPA build (includes this plugin)
make package    # uv build marker wheel
```

## Layout

```
src/
  api/mlOps.ts          # ML REST client (merged into opsApi)
  routes.tsx            # ML React Router routes
  nav.tsx               # Sidebar sections
  features/ops/
    metrics/
      models/           # model detail, metrics table, add candidate
      frozen-datasets/  # frozen dataset pages & tables
      classes/          # per-class metrics page
      MetricsOverviewPage.tsx, metricsSchema.ts, …
    training/
    images/
  types/
```

## Tests

No separate Jest project — tests live in `libs/datapipe-ui/src/` and import `@datapipe/ui-ml/...`.

CI: `.github/workflows/lib-datapipe-ui.yml`
