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

This time nothing is reprocessed — datapipe sees that the source data hasn't changed and skips both steps. This is the core behaviour: work is only done when it needs to be.

## See the step list

```bash
datapipe step list
```

This shows all steps in your pipeline and how many records are pending for each.

## What just happened

- `BatchGenerate` is a special step that populates a table from an external source (here, a Python generator). It runs in full each time and datapipe detects which rows changed.
- `BatchTransform` receives a `pd.DataFrame` of the rows that need processing and returns a `pd.DataFrame` of results. Datapipe tracks the `update_ts` / `process_ts` pair for every record to determine what to pass in.
- The metadata (which rows were processed, when, with what result) is stored in the SQLite file alongside your data tables.

## Next steps

- Read [Concepts](../concepts/what-is-datapipe.md) to understand how incremental tracking works.
- See [How to pull data from external sources](../how-to/external-sources.md) for more `BatchGenerate` patterns.
- See [How to run model inference](../how-to/model-inference.md) for multi-input transforms.
