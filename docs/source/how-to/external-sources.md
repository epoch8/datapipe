# How to Pull Data from External Sources

Bring data that Datapipe does not write itself into the incremental graph — either by generating rows in a step, or by syncing meta for tables owned elsewhere.

## Goal

Choose the right step when the source of truth lives outside Datapipe (files on disk, rows inserted by another service, an API, a feed).

## When data is owned outside Datapipe

| Situation | Use |
|---|---|
| Another process writes files or rows; Datapipe only needs to notice changes | [`UpdateExternalTable`](../reference/steps/update-external-table.md) |
| Your pipeline pulls or synthesizes batches (API, scrape, seed data) | [`BatchGenerate`](../reference/steps/batch-generate.md) |

Both update meta so downstream `BatchTransform` steps see inserts, updates, and deletes. Neither is a substitute for a transform that derives new tables from existing ones.

## Steps

### Option A — Sync an externally owned table

Point a `Table` at the external store, then refresh meta:

```python
from datapipe.compute import Table
from datapipe.step.update_external_table import UpdateExternalTable
from datapipe.store.filedir import PILFile, TableStoreFiledir

incoming = Table(
    name="incoming",
    store=TableStoreFiledir("dropbox/{id}.png", PILFile("png")),  # or any TableStore
)

pipeline_steps = [
    UpdateExternalTable(output=incoming),
    # BatchTransform(... inputs=[incoming] ...)
]
```

On each run, Datapipe scans the store, recomputes hashes, and marks changed / deleted keys dirty. It does **not** invent rows — the store must already contain them.

### Option B — Generate batches yourself

Yield DataFrames from a generator. Datapipe writes them to the output table(s) and tracks hashes:

```python
import pandas as pd
from datapipe.step.batch_generate import BatchGenerate

def pull_feed():
    # call API, list S3 keys, read a dump, …
    yield pd.DataFrame([
        {"item_id": 1, "payload": "..."},
        {"item_id": 2, "payload": "..."},
    ])

BatchGenerate(pull_feed, outputs=[Items])
```

Yield multiple chunks if the source is large. Unchanged content keeps the same hash and does not dirty downstream work.

### 3. Run and verify

```bash
datapipe run
datapipe step list --status
```

Downstream transforms should show dirty keys only for new or changed source rows.

## Expected result

- External inserts/updates appear as dirty keys for dependent steps.
- Deletes (missing files / removed rows) propagate through the graph.
- You do not hand-manage “processed” flags in application code.

## Choosing quickly

- **Files dropped by users or another service** → `UpdateExternalTable` (see [Transform Files](./transform-files.md)).
- **You control the fetch** and want Datapipe to store the result → `BatchGenerate`.
- **Both**: generate into an intermediate table, or sync one table and generate another — mix freely in one `Pipeline`.

## See also

- Example with files: [`examples/datapipe_core/image_resize/`](https://github.com/epoch8/datapipe/tree/master/examples/datapipe_core/image_resize)
- Example with generate: [`examples/datapipe_core/one_to_many_pipeline/`](https://github.com/epoch8/datapipe/tree/master/examples/datapipe_core/one_to_many_pipeline)
- [Pipeline Steps](../concepts/pipeline-steps.md)
