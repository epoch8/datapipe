# This is copy of concept of reusable test classes from `fsspec`
# https://github.com/fsspec/filesystem_spec/tree/master/fsspec/tests/abstract

from typing import Tuple

import cloudpickle
import pandas as pd
import pytest

from datapipe.store.table_store import TableStore
from datapipe.tests.util import assert_ts_contains
from datapipe.types import DataSchema


class AbstractBaseStoreFixtures:
    @pytest.fixture
    def store(self):
        raise NotImplementedError("This function must be overridden in derived classes")


class AbstractBaseStoreTests:
    def test_cloudpickle(self, store: TableStore) -> None:
        ser = cloudpickle.dumps(store)
        cloudpickle.loads(ser)

        # TODO assert store is the same


class AbstractReadWriteStoreFixtures(AbstractBaseStoreFixtures):
    @pytest.fixture
    def schema_df(self) -> Tuple[DataSchema, pd.DataFrame]:
        raise NotImplementedError("This function must be overridden in derived classes")

    @pytest.fixture
    def store_schema_df(
        self,
        schema_df: Tuple[DataSchema, pd.DataFrame],
    ) -> Tuple[TableStore, DataSchema, pd.DataFrame]:
        raise NotImplementedError("This function must be overridden in derived classes")


class AbstractReadWriteStoreTests:
    def test_write_read_rows(
        self,
        store_schema_df: Tuple[TableStore, DataSchema, pd.DataFrame],
    ) -> None:
        store, schema, test_df = store_schema_df
        store.insert_rows(test_df)

        assert_ts_contains(store, test_df)
