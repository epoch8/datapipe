# Datapipe

# Getting Started

- [Introduction](./getting-started/introduction.md)
- [Installation](./getting-started/installation.md)
- [Your First Pipeline](./getting-started/first-pipeline.md)
- [E2E Image Detection Walkthrough](./getting-started/e2e-image-detection-walkthrough.md)
- [Troubleshooting](./troubleshooting.md)

# Concepts

- [Concepts overview](./concepts/index.md)
- [What is Datapipe?](./concepts/what-is-datapipe.md)
- [Incremental Processing](./concepts/incremental-processing.md)
- [Output cleanup and processed_idx](./concepts/processed-idx.md)
- [Soft delete and resurrection](./concepts/soft-delete.md)
- [Transform grain (transform_keys)](./concepts/transform-grain.md)
- [BatchGenerate vs BatchTransform](./concepts/generate-vs-transform.md)
- [The idx parameter on transforms](./concepts/idx-parameter.md)
- [Tables and TableStores](./concepts/tables-and-stores.md)
- [Pipeline Steps](./concepts/pipeline-steps.md)
- [Primary Keys and Transform Keys](./concepts/primary-keys.md)

# How-to Guides

- [Transform Files (1-to-1)](./how-to/transform-files.md)
- [Pull Data from External Sources](./how-to/external-sources.md)
- [Run Model Inference (Multi-Input Transforms)](./how-to/model-inference.md)
- [Expand One Row Into Many (1-to-N)](./how-to/one-to-many.md)
- [Map Mismatched Primary Keys](./how-to/key-mapping.md)
- [Filter Steps by Labels](./how-to/filter-by-labels.md)
- [Run Detection Tags Pipeline](./how-to/run-detection-tags-pipeline.md)
- [Serve with DatapipeAPI](./how-to/serve-with-datapipe-api.md)
- [Use SQLite as Metadata Store](./how-to/using-sqlite.md)
- [Use PostgreSQL in Production](./how-to/production-postgres.md)
- [Manage Schema Changes with Alembic](./how-to/alembic-migrations.md)
- [Run Steps with RayExecutor](./how-to/ray-executor.md)
- [Extend the CLI](./how-to/extend-cli.md)
- [Write a Custom TableStore](./how-to/custom-table-store.md)

# Reference

- [CLI Commands](./reference/cli.md)
- [Pipeline / Catalog / DatapipeApp](./reference/pipeline-catalog.md)
- [ComputeStep](./reference/compute-step.md)
- [Table / DataStore / DataTable](./reference/table.md)
- [TableMeta / TransformMeta](./reference/meta.md)
- [RunConfig / Callbacks / Cancel](./reference/run-config.md)
- [Steps](./reference/steps/index.md)
  - [BatchTransform](./reference/steps/batch-transform.md)
  - [BatchGenerate](./reference/steps/batch-generate.md)
  - [UpdateExternalTable](./reference/steps/update-external-table.md)
  - [DatatableTransform](./reference/steps/datatable-transform.md)
  - [ML steps (index)](./reference/steps/ml/index.md)
    - [Freeze dataset](./reference/steps/ml/freeze-dataset.md)
    - [Detection — inference](./reference/steps/ml/detection/inference.md)
    - [Detection — train YOLOv8](./reference/steps/ml/detection/train-yolov8.md)
- [TableStore Backends](./reference/stores/index.md)
  - [Database](./reference/stores/database.md)
  - [Filedir](./reference/stores/filedir.md)
  - [Redis](./reference/stores/redis.md)
  - [Elasticsearch](./reference/stores/elastic.md)
  - [Qdrant](./reference/stores/qdrant.md)
  - [Milvus](./reference/stores/milvus.md)
- [Types](./reference/types.md)
- [Executors](./reference/executors.md)

# Ops (App / UI)

- [Overview](./ops/index.md)
- [Install and run Ops](./ops/install-and-run.md)
- [DatapipeApp and API](./ops/datapipe-app.md)
- [Ops specs (ML panels)](./ops/ops-specs.md)
- [Ops UI walkthrough](./ops/ui-walkthrough.md)
- [Observability and run logs](./ops/observability.md)

# Integrations

- [Overview](./integrations/index.md)
- [ML mental model](./integrations/ml-overview.md)
- [datapipe-ml index](./integrations/datapipe-ml.md)
- [Label Studio](./integrations/label-studio.md)
- [CVAT](./integrations/cvat.md)
- [FiftyOne and OCR](./integrations/appendices.md)

# Explanation

- [Architecture overview](./explanation/architecture.md)
- [Compute Step Lifecycle](./explanation/compute-step-lifecycle.md)
- [Change Detection and Merging](./explanation/change-detection.md)
- [Meta-Table Schema](./explanation/meta-table-schema.md)

# Migration

- [v0.13 &rarr; v0.14](./migration/v013-to-v014.md)
- [v0.14 &rarr; v0.15](./migration/v014-to-v015.md)
