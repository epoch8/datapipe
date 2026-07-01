# Introduction

Datapipe is a Python framework for **durable, incremental batch processing**.

You define a pipeline as a graph of tables connected by transform functions. Datapipe tracks dependencies at the record level: when a row in an input table changes, only the downstream computations that depend on that specific row are re-run. Everything else is skipped. Processing state is persisted to a metadata store, so a pipeline interrupted mid-run picks up where it left off on the next execution.

## What problems does it solve?

Most data processing tasks involve running the same logic repeatedly as source data grows or changes. Without incremental tracking, you have two unappealing options: reprocess everything on every run (expensive) or write custom change-detection logic yourself (fragile and tedious).

Datapipe handles the change-detection bookkeeping so your transform functions stay simple and stateless — they receive a `pd.DataFrame` of rows to process and return a `pd.DataFrame` of results. Datapipe takes care of figuring out which rows those should be.

## What it is good for

- **File and media processing** — resize images, transcode video, extract text; only re-process files that have changed.
- **ML inference pipelines** — run a model over a dataset; automatically re-infer when the model or the input data changes.
- **Data enrichment** — join, filter, and reshape records across multiple source tables; propagate changes incrementally through the graph.
- **External data synchronisation** — pull from APIs or databases periodically; only downstream steps that are affected by new or updated records are re-triggered.

## What it is not

Datapipe is not a streaming engine. It processes data in batches (pandas DataFrames) and is designed for workloads where latency of seconds to minutes is acceptable. It is also not a distributed compute engine — for large-scale parallelism, it integrates with Ray via `RayExecutor`.

## Prerequisites

- Python 3.10+
- SQLAlchemy 2.0 (used for defining table schemas and the metadata store)
