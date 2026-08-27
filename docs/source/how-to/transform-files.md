# How to Transform Files (1-to-1)

Resize, convert, or otherwise process files one-to-one so each input key maps to one output key — and deletes propagate when sources disappear.

## Goal

Build a pipeline that watches an external file directory, runs a per-row `BatchTransform`, and writes results to another file store.

## Prerequisites

- Python **3.10+**
- `datapipe-core` with file store support
- A directory of input files (images or other formats supported by a file adapter)

Install for local development:

```bash
pip install "datapipe-core[sqlite]"
```

## Example repo

| Example | What it demonstrates |
|---------|---------------------|
| [`examples/datapipe_core/image_resize/`](https://github.com/epoch8/datapipe/tree/master/examples/datapipe_core/image_resize) | Minimal 1-to-1 image resize with `TableStoreFiledir` |
| [`examples/e2e_template/image_detection/steps.py`](https://github.com/epoch8/datapipe/tree/master/examples/e2e_template/image_detection/steps.py) | `download_images` — S3 URL → local file (same primary key `image_name`) |

The e2e detection template downloads remote images to disk before FiftyOne export:

```python
BatchTransform(
    func=steps.download_images,
    inputs=["s3_images"],
    outputs=["local_images"],
    transform_keys=["image_name"],
    labels=[("stage", "fiftyone")],
    kwargs=dict(
        image__image_path__name="image_url",
        image__local_image_path__name="local_path",
    ),
)
```

Each `image_name` maps to one local file. Unchanged URLs are skipped on re-run. See [Incremental Processing](../concepts/incremental-processing.md).

## Steps

### 1. Declare file tables

Use `TableStoreFiledir` with a path template and a file adapter (for example `PILFile` for images):

```python
from datapipe.compute import Table
from datapipe.store.filedir import PILFile, TableStoreFiledir

input_images = Table(
    name="input_images",
    store=TableStoreFiledir("input/{id}.jpeg", PILFile("jpg")),
)

resized = Table(
    name="preprocessed_images",
    store=TableStoreFiledir("output/{id}.png", PILFile("png")),
)
```

Primary keys come from the path template (`{id}` above). Input and output share the same key grain for a true 1-to-1 transform. See [Primary Keys and Transform Keys](../concepts/primary-keys.md).

### 2. Sync external files into meta

Files appear outside Datapipe (uploads, another service, a bucket). Refresh meta with `UpdateExternalTable` so inserts, updates, and deletes become dirty keys:

```python
from datapipe.step.update_external_table import UpdateExternalTable

UpdateExternalTable(output=input_images)
```

See [Pull Data from External Sources](./external-sources.md).

### 3. Transform each row

Write a DataFrame → DataFrame function. File adapters put file payloads in a column (here `image`); keep the primary key columns in the result:

```python
import pandas as pd
from datapipe.step.batch_transform import BatchTransform

def resize(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["image"] = df["image"].apply(lambda im: im.resize((50, 50)))
    return df

BatchTransform(
    resize,
    inputs=[input_images],
    outputs=[resized],
    chunk_size=100,
)
```

### 4. Wire the app and run

```python
from datapipe.compute import Catalog, DatapipeApp, Pipeline
from datapipe.datatable import DataStore
from datapipe.store.database import DBConn

pipeline = Pipeline([
    UpdateExternalTable(output=input_images),
    BatchTransform(resize, inputs=[input_images], outputs=[resized], chunk_size=100),
])

ds = DataStore(DBConn("sqlite+pysqlite3:///db.sqlite"))
app = DatapipeApp(ds, Catalog({}), pipeline)
```

```bash
datapipe db create-all
datapipe run
```

## Verify

After adding or changing files under `input/`:

```bash
datapipe run
ls output/
```

- New or changed input files produce matching outputs under the same primary key.
- Removed inputs cause Datapipe to delete the corresponding output files on the next run.
- Re-run with unchanged inputs — log should show 0 batches for the transform step.

```bash
datapipe table preprocessed_images list
```

## Expected result

- New or changed input files produce matching outputs under the same primary key.
- Removed inputs cause Datapipe to delete the corresponding output files on the next run.
- Unchanged keys are skipped on subsequent runs.

## See also

- [Pull Data from External Sources](./external-sources.md)
- [Tables and TableStores](../concepts/tables-and-stores.md)
- [BatchTransform](../reference/steps/batch-transform.md)
- [E2E Image Detection Walkthrough](../getting-started/e2e-image-detection-walkthrough.md) — `download_images` in context
