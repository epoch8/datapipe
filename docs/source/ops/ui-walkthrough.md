# Ops UI walkthrough

What the dashboard shows, how the ML plugin mounts, and how to drive work from CLI + browser.

## What you get

With `datapipe-app[ui]` (or `[ml]`) and built static assets, `datapipe api` serves a React SPA.

### Core UI (`datapipe-ui`)

| Area | What it shows |
|---|---|
| **Pipeline** | Steps, labels, and connectivity for the bound `Pipeline` |
| **Graph** | Visual DAG of tables and transforms |
| **Runs** | History of pipeline / step executions (when observability is configured) |
| **Tables** | Catalog tables and row samples |
| **Transforms** | Step-oriented views into compute steps |

### Routes

| Path | Role |
|---|---|
| `/` | Main Ops SPA (pipeline, runs, tables, transforms) |
| `/graph` | Dedicated debug graph view |

`DatapipeAPI` mounts static files from the resolved UI package and falls back to SPA `index.html` for client routes.

### ML plugin (`datapipe-ui-ml`)

When `datapipe-app[ml]` is installed and the ML SPA is built, extra pages appear: metrics, models, frozen datasets, training runs, image browsers, ops-spec panels.

Frontend resolution prefers the ML package: `datapipe_app.app.frontend_static.resolve_frontend_dir()` loads entry points in order `datapipe_ui_ml`, then `datapipe_ui` (`datapipe.ui_static` group). So **`datapipe-ui-ml` is preferred when both are present**.

ML API / specs come from `datapipe-app-ml-ops`, not from the SPA alone.

## Typical session

1. Start the API — see [Install and run](./install-and-run.md).
2. Open `/` — confirm the graph matches your `Pipeline`.
3. Trigger work from the CLI:

   ```bash
   datapipe --pipeline app:app run
   # or stage-filtered:
   datapipe --pipeline app:app step --labels=stage=train run
   ```

4. Refresh the UI — open **Runs** for success / failure and logs ([Observability](./observability.md)).
5. Open a **Table** — verify incremental outputs after an insert/update.

Some examples also expose run actions from the UI; CLI remains the reliable trigger everywhere.

## Local UI development

From the monorepo:

```bash
make -C libs/datapipe-ui install
make -C libs/datapipe-ui start      # CRA / Vite-style dev server for core UI

# ML plugin host (when working on ML pages)
make -C libs/datapipe-ui-ml start
```

Production packaging copies `build/` into the Python package static dir (`scripts/copy-static.sh` / `make … build-package`). The API serves those files — not the dev server — when you run `datapipe api`.

## See also

- `libs/datapipe-ui/README.md`
- `libs/datapipe-ui-ml/README.md`
- Core example: `examples/datapipe_app/`
- ML-heavy: `examples/detection_tags/`, `examples/e2e_template/`
