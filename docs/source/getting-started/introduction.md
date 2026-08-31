# Introduction

Datapipe is a Python framework for **durable, incremental batch processing**.

You define a pipeline as a graph of tables connected by transform functions. Datapipe tracks dependencies at the **record** level: when a row changes, only downstream work for that key re-runs. Everything else is skipped. State lives in a SQL metadata store, so an interrupted run resumes cleanly.

## The feature that matters

**Incremental processing** — insert, update, delete, and “wrote the same bytes again” — is explained with short animations here:

→ **[Incremental Processing](../concepts/incremental-processing.md)**

If you only read one concept page, read that one.

## What problems does it solve?

Most batch jobs either reprocess everything (expensive) or grow custom change-detection code (fragile). Datapipe owns the bookkeeping. Your functions stay simple: receive a `pd.DataFrame`, return a `pd.DataFrame`.

## Good fits

- File and media processing (resize, transcode, OCR) — only changed files
- ML inference graphs — re-infer when model **or** inputs change
- Enrichment joins across tables — propagate changes through the graph
- External sync — pull APIs/DBs periodically; wake only affected downstream steps

## Not a fit

- Sub-second streaming / windowing
- “Always scan the full table” analytics (use a warehouse; or `DatatableTransform` if you must)

## Prerequisites

- Python 3.10+
- SQLAlchemy 2.0 (schemas + metadata store)

## Next steps

1. [Installation](./installation.md)
2. [Your First Pipeline](./first-pipeline.md)
3. [Incremental Processing](../concepts/incremental-processing.md) — the four table-panel cases
4. [Troubleshooting](../troubleshooting.md) — when the second run “does nothing”
