# How to Transform Files (1-to-1)

Resize, convert, or otherwise process files one-to-one so each input key maps to one output key — and deletes propagate when sources disappear.

## Goal

Build a pipeline that watches an external file directory, runs a per-row `BatchTransform`, and writes results to another file store.

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

Primary keys come from the path template (`{id}` above). Input and output share the same key grain for a true 1-to-1 transform.

### 2. Sync external files into meta

Files appear outside Datapipe (uploads, another service, a bucket). Refresh meta with `UpdateExternalTable` so inserts, updates, and deletes become dirty keys:

```python
from datapipe.step.update_external_table import UpdateExternalTable

UpdateExternalTable(output=input_images)
```

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

## Expected result

- New or changed input files produce matching outputs under the same primary key.
- Removed inputs cause Datapipe to delete the corresponding output files on the next run.
- Unchanged keys are skipped on subsequent runs.

## Example

Full pipeline: [`examples/datapipe_core/image_resize/`](https://github.com/epoch8/datapipe/tree/master/examples/datapipe_core/image_resize).

## See also

- [Pull Data from External Sources](./external-sources.md)
- [Tables and TableStores](../concepts/tables-and-stores.md)
- [BatchTransform](../reference/steps/batch-transform.md)
