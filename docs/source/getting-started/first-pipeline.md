# Your First Pipeline

This guide walks through building a minimal pipeline that demonstrates datapipe's core behaviour: running only the work that needs to be done.

## What we'll build

A pipeline with two steps:

1. **Generate** a small table of words.
2. **Transform** each word into its character count.

When a word changes, only its downstream computation re-runs. Everything unchanged is skipped.

## Prerequisites

Install datapipe with the SQLite extra for local development:

```bash
pip install "datapipe-core[sqlite]"
```

## The pipeline

Create a file `app.py`:

```python
import pandas as pd
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from datapipe.compute import Catalog, DatapipeApp, Pipeline
from datapipe.datatable import DataStore
from datapipe.step.batch_generate import BatchGenerate
from datapipe.step.batch_transform import BatchTransform
from datapipe.store.database import DBConn


class Base(DeclarativeBase):
    pass


class Word(Base):
    __tablename__ = "words"

    word_id: Mapped[int] = mapped_column(primary_key=True)
    text: Mapped[str]


class WordLength(Base):
    __tablename__ = "word_lengths"

    word_id: Mapped[int] = mapped_column(primary_key=True)
    length: Mapped[int]


def generate_words():
    yield pd.DataFrame([
        {"word_id": 1, "text": "hello"},
        {"word_id": 2, "text": "world"},
        {"word_id": 3, "text": "datapipe"},
    ])


def compute_lengths(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(length=df["text"].str.len())[["word_id", "length"]]


pipeline = Pipeline([
    BatchGenerate(generate_words, outputs=[Word]),
    BatchTransform(
        compute_lengths,
        inputs=[Word],
        outputs=[WordLength],
    ),
])

dbconn = DBConn("sqlite+pysqlite3:///first_pipeline.sqlite", sqla_metadata=Base.metadata)
ds = DataStore(dbconn)
app = DatapipeApp(ds, Catalog({}), pipeline)
```

## Run it

**Create the database tables** (do this once):

```bash
datapipe db create-all
```

**Run the pipeline:**

```bash
datapipe run
```

You should see both steps execute: `generate_words` fills the `words` table, then `compute_lengths` produces a row in `word_lengths` for each word.

**Run again:**

```bash
datapipe run
```

`BatchGenerate` still runs its generator loop and writes the same rows, but identical content keeps the same hash — so `update_ts` does not move and `BatchTransform` has no dirty keys to process. This is the core behaviour: transforms run only when inputs actually change.

## See the step list

```bash
datapipe step list
```

This shows all steps in your pipeline and how many records are pending for each.

## What just happened

- `BatchGenerate` seeds the `words` table. On the second run it still executes, but same-hash writes leave no dirty keys for downstream work.
- `BatchTransform` only receives keys where input `update_ts` is newer than the step `process_ts` (see [Incremental Processing](../concepts/incremental-processing.md)) — so it skips when nothing changed.
- Try changing one word in `generate_words` and re-running: only that `word_id` recomputes in `word_lengths`.

## Next steps

- **[Incremental Processing](../concepts/incremental-processing.md)** — insert / update / delete / unchanged table panels
- [What is Datapipe?](../concepts/what-is-datapipe.md)
- [Pull data from external sources](../how-to/external-sources.md)
- [Run model inference](../how-to/model-inference.md) — multi-input transforms
