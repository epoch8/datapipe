# Datapipe

# Getting Started

- [Introduction](./getting-started/introduction.md)
- [Installation](./getting-started/installation.md)
- [Your First Pipeline](./getting-started/first-pipeline.md)

# Concepts

- [What is Datapipe?](./concepts/what-is-datapipe.md)
- [Tables and TableStores](./concepts/tables-and-stores.md)
- [Pipeline Steps](./concepts/pipeline-steps.md)
- [Incremental Processing](./concepts/incremental-processing.md)
- [Primary Keys and Transform Keys](./concepts/primary-keys.md)

# How-to Guides

- [Transform Files (1-to-1)](./how-to/transform-files.md)
- [Pull Data from External Sources](./how-to/external-sources.md)
- [Run Model Inference (Multi-Input Transforms)](./how-to/model-inference.md)
- [Expand One Row Into Many (1-to-N)](./how-to/one-to-many.md)
- [Map Mismatched Primary Keys](./how-to/key-mapping.md)
- [Filter Steps by Labels](./how-to/filter-by-labels.md)
- [Use SQLite as Metadata Store](./how-to/using-sqlite.md)
- [Extend the CLI](./how-to/extend-cli.md)
- [Write a Custom TableStore](./how-to/custom-table-store.md)

# Reference

- [CLI Commands](./reference/cli.md)
- [Pipeline / Catalog / DatapipeApp](./reference/pipeline-catalog.md)
- [Table](./reference/table.md)
- [Steps](./reference/steps/index.md)
  - [BatchTransform](./reference/steps/batch-transform.md)
  - [BatchGenerate](./reference/steps/batch-generate.md)
  - [UpdateExternalTable](./reference/steps/update-external-table.md)
  - [DatatableTransform](./reference/steps/datatable-transform.md)
- [TableStore Backends](./reference/stores/index.md)
  - [Database](./reference/stores/database.md)
  - [Filedir](./reference/stores/filedir.md)
  - [Redis](./reference/stores/redis.md)
  - [Elasticsearch](./reference/stores/elastic.md)
  - [Qdrant](./reference/stores/qdrant.md)
  - [Milvus](./reference/stores/milvus.md)
- [Types](./reference/types.md)
- [Executors](./reference/executors.md)

# Explanation

- [Compute Step Lifecycle](./explanation/compute-step-lifecycle.md)
- [Change Detection and Merging](./explanation/change-detection.md)
- [Meta-Table Schema](./explanation/meta-table-schema.md)

# Migration

- [v0.13 &rarr; v0.14](./migration/v013-to-v014.md)
