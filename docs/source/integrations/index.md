# Integrations: ML, Label Studio, CVAT

Beyond core ETL, the monorepo ships first-class addons for computer-vision / ML ops loops.

| Package | Role |
|---|---|
| `datapipe-ml` | Train / infer / metrics / freeze datasets / FiftyOne helpers |
| `datapipe-label-studio` | Projects, tasks, predictions sync with Label Studio |
| `datapipe-cvat` | CVAT step integration |
| `datapipe-app-ml-ops` + `datapipe-ui-ml` | ML Ops API panels and UI plugin |

All of these still sit on **incremental** `PipelineStep`s — dirty keys drive re-train / re-infer / re-upload work.

## Guides

1. [ML mental model](./ml-overview.md) — stages and examples
2. [datapipe-ml reference index](./datapipe-ml.md) — module map
3. [Label Studio](./label-studio.md)
4. [CVAT](./cvat.md)
5. [FiftyOne and OCR appendices](./appendices.md)

Flagship examples: `examples/detection_tags/`, `examples/e2e_template/`.
