# How to Write a Custom TableStore

Add a storage backend Datapipe does not ship — any system you can read and write as keyed DataFrames.

## Goal

Implement `TableStore`, declare capabilities, and reuse the shared store test suite.

## When you need this

Use a custom store when data must live in a database or service that is not covered by the built-in backends (SQL, filedir, Redis, Elasticsearch, Qdrant, Milvus, …).

## Steps

### 1. Subclass `TableStore`

Implement the abstract surface your backend supports. Required methods (from `datapipe.store.table_store.TableStore`):

- `get_primary_schema` / `get_meta_schema` / `get_schema`
- `insert_rows` / `delete_rows` / `read_rows`
- Optional: override `update_rows` (default is delete-then-insert) and `read_rows_meta_pseudo_df`

Tiny in-memory sketch:

```python
import pandas as pd
from sqlalchemy import Column, Integer, String

from datapipe.store.table_store import TableStore, TableStoreCaps
from datapipe.types import DataDF, DataSchema, IndexDF, MetaSchema, data_to_index


class InMemoryStore(TableStore):
    caps = TableStoreCaps(
        supports_delete=True,
        supports_get_schema=True,
        supports_read_all_rows=True,
        supports_read_nonexistent_rows=True,
        supports_read_meta_pseudo_df=True,
    )

    def __init__(self, data_schema: DataSchema | None = None) -> None:
        self._schema: DataSchema = data_schema or [
            Column("id", String(), primary_key=True),
            Column("value", Integer()),
        ]
        self._df = pd.DataFrame(columns=[c.name for c in self._schema])

    def get_primary_schema(self) -> DataSchema:
        return [c for c in self._schema if c.primary_key]

    def get_meta_schema(self) -> MetaSchema:
        return []

    def get_schema(self) -> DataSchema:
        return list(self._schema)

    def read_rows(self, idx: IndexDF | None = None) -> DataDF:
        if idx is None:
            return self._df.copy()
        if len(idx) == 0:
            return pd.DataFrame(columns=[c.name for c in self._schema])
        keys = self.primary_keys
        left = self._df.set_index(keys)
        right = idx.set_index(keys)
        return left.loc[left.index.intersection(right.index)].reset_index()

    def insert_rows(self, df: DataDF) -> None:
        if df.empty:
            return
        self.delete_rows(data_to_index(df, self.primary_keys))
        self._df = pd.concat([self._df, df], ignore_index=True)

    def delete_rows(self, idx: IndexDF) -> None:
        if idx.empty or self._df.empty:
            return
        keys = self.primary_keys
        left = self._df.set_index(keys)
        right = idx.set_index(keys)
        self._df = left.loc[left.index.difference(right.index)].reset_index()
```

Set each `TableStoreCaps` flag honestly: the abstract tests skip cases your store cannot support (for example `supports_read_all_rows=False`).

### 2. Support external sync if needed

If the table is filled outside Datapipe and you use `UpdateExternalTable`, implement `read_rows_meta_pseudo_df` so Datapipe can discover keys and content for hashing. The base class yields `read_rows()` once; override for true chunking on large stores.

### 3. Wire it into a `Table`

```python
from datapipe.compute import Table

my_table = Table(name="events", store=InMemoryStore(...))
```

Use that table as a catalog entry or as a step input/output like any built-in store.

### 4. Run the abstract test suite

```python
import pytest
from sqlalchemy import Column, String

from datapipe.store.tests.abstract import AbstractBaseStoreTests
from datapipe.types import DataSchema

class TestInMemoryStore(AbstractBaseStoreTests):
    @pytest.fixture
    def store_maker(self):
        def make_store(data_schema: DataSchema):
            return InMemoryStore(data_schema=data_schema)

        return make_store
```

Subclassing `AbstractBaseStoreTests` and providing `store_maker` runs the shared pytest cases (round-trip read/write, deletes, cloudpickle, optional full-table reads, and so on).

## Expected result

- Pipeline steps can read and write your backend through the normal DataFrame APIs.
- Caps and tests document what the store guarantees.
- Incremental meta tracking works as long as primary keys and hashes are stable.

## See also

- [Tables and TableStores](../concepts/tables-and-stores.md)
- [TableStore backends reference](../reference/stores/index.md)
- Built-in example of the test pattern: `TestTableStoreFiledir` in `libs/datapipe-core/tests/test_table_store_filedir.py`
