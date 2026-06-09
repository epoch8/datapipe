# What is Datapipe?

Datapipe is a Python framework for durable, incremental batch processing. It lets you define a data processing graph once, then run it repeatedly as data changes — processing only what needs to be processed.

## The core idea

A Datapipe pipeline is a directed graph of **Tables** connected by **Steps**. Each step is a Python function that receives one or more `pd.DataFrame`s as input and produces one or more `pd.DataFrame`s as output.

What makes this different from a plain script is what Datapipe tracks between runs:

- For each record in every table, it knows the last time that record was **updated** (`update_ts`).
- For each step, it knows the last time each record was **processed** (`process_ts`).
- Before running a step, Datapipe computes the set of records where `update_ts > process_ts` across all inputs. Only those records are passed to your function.

This means your transform functions are always simple and stateless. They do not need to know which records are "new" — Datapipe handles that entirely.

## Durability

Processing state is written to a SQL metadata store after each successful batch. If a pipeline is interrupted — by a crash, a deployment, or a manual stop — the next run resumes from where it left off. No records are skipped or double-processed.

This makes Datapipe suitable for long-running jobs over large datasets where reliability matters.

## Batch orientation

The unit of work in Datapipe is a `pd.DataFrame` batch, not a single row and not a stream event. The `chunk_size` parameter on `BatchTransform` controls how many rows are included per batch. This allows you to tune memory usage and throughput independently.

## What Datapipe is not

- **Not a streaming engine.** There is no concept of low-latency event processing or windowing. Runs are triggered explicitly.
- **Not a distributed compute engine.** By default, steps run single-threaded. A `RayExecutor` is available for parallelism across steps.
- **Not opinionated about storage.** Tables can live in a SQL database, on the filesystem, in Redis, Elasticsearch, Qdrant, Milvus, or a custom backend — as long as you provide a `TableStore` implementation.
