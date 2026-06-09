# Pipeline Steps

> **Needs review.** This page was carried over from the previous documentation and has not been updated yet.

## Patterns of data flow

### Generating data from an external source

Examples:
- Parsing a product feed (e.g. YML) and populating a table
- Calling an external API to retrieve a list of items

Generation runs in batches; the total volume is not known in advance.

### Batch transformation 1-to-1 (no dependency on all data)

Examples:
- Resizing images
- Running ML model inference

### Batch transformation 1-to-N or N-to-1 on small batches

Examples:
- Expanding product attributes into individual records: `(product_id)` &rarr; `(product_id, attribute_id)`
- Aggregating classified bounding boxes into one record per image: `(image_id, bbox_id)` &rarr; `(image_id)`

### Global (or near-global) transformation

> Data may be read multiple times. The total volume may be too large to fit in memory at once.

Example: training an ML model on a full table.

---

## ComputeStep types

### `DatatableTransform`

Accepts a list of input and output `DataTable`s and applies an external function to them.

This type gives Datapipe no visibility into which individual records changed, so it cannot perform incremental (`Changelist`) processing. Use it for generation steps and global transforms such as model training.

### `BatchTransform`

Accepts a function `func` together with input and output tables. Datapipe uses record-level metadata to determine which rows need reprocessing and passes only those rows to `func` in chunks.

Suitable for incremental (`Changelist`) processing.

Covers 1-to-1 batch transforms and small-batch 1-to-N / N-to-1 patterns.

**Magic injection** — Datapipe inspects the signature of `func` and automatically supplies:
- `ds` → the active `DataStore`
- `run_config` → the current `RunConfig`
- `idx` → an `IndexDF` containing the primary keys of the current batch

### `BatchGenerate`

Accepts a generator function `func` and output tables `outputs`. Use this step when you need to define primary (source) tables or periodically synchronise data from an external source (another database table, files on disk, etc.).

**Magic injection:**
- `ds` → the active `DataStore`
