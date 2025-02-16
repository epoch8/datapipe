# This is copy of concept of reusable test classes from `fsspec`
# https://github.com/fsspec/filesystem_spec/tree/master/fsspec/tests/abstract

from typing import Callable, cast

import cloudpickle
import pandas as pd
import pytest
from sqlalchemy import Column, String

from datapipe.store.table_store import TableStore
from datapipe.store.tests.stubs import DATA_PARAMS
from datapipe.tests.util import assert_df_equal, assert_ts_contains
from datapipe.types import DataSchema, IndexDF, data_to_index

TableStoreMaker = Callable[[DataSchema], TableStore]


class AbstractBaseStoreFixtures:
    @pytest.fixture
    def store_maker(self) -> TableStoreMaker:
        raise NotImplementedError("This function must be overridden in derived classes")


class AbstractBaseStoreTests:
    def test_cloudpickle(self, store_maker: TableStoreMaker) -> None:
        store = store_maker(
            [
                Column("id", String(), primary_key=True),
            ]
        )
        ser = cloudpickle.dumps(store)
        cloudpickle.loads(ser)

        # TODO assert store is the same

    @pytest.mark.parametrize("data_df,schema", DATA_PARAMS)
    def test_get_schema(
        self,
        store_maker: TableStoreMaker,
        data_df: pd.DataFrame,
        schema: DataSchema,
    ) -> None:
        store = store_maker(schema)

        if not store.caps.supports_get_schema:
            raise pytest.skip("Store does not support get_schema")

        assert store.get_schema() == schema

    @pytest.mark.parametrize("data_df,schema", DATA_PARAMS)
    def test_write_read_rows(
        self,
        store_maker: TableStoreMaker,
        data_df: pd.DataFrame,
        schema: DataSchema,
    ) -> None:
        store = store_maker(schema)
        store.insert_rows(data_df)

        assert_ts_contains(store, data_df)

    @pytest.mark.parametrize("data_df,schema", DATA_PARAMS)
    def test_write_read_full_rows(
        self, store_maker: TableStoreMaker, data_df: pd.DataFrame, schema: DataSchema
    ) -> None:
        store = store_maker(schema)

        if not store.caps.supports_read_all_rows:
            raise pytest.skip("Store does not support read_all_rows")

        store.insert_rows(data_df)

        assert_df_equal(store.read_rows(), data_df, index_cols=store.primary_keys)

    @pytest.mark.parametrize("data_df,schema", DATA_PARAMS)
    def test_insert_identical_rows_twice_and_read_rows(
        self,
        store_maker: TableStoreMaker,
        data_df: pd.DataFrame,
        schema: DataSchema,
    ) -> None:
        store = store_maker(schema)

        store.insert_rows(data_df)

        test_df_mod = data_df.copy()
        test_df_mod.loc[50:, "price"] = test_df_mod.loc[50:, "price"] + 1

        store.insert_rows(test_df_mod.loc[50:])

        assert_ts_contains(store, test_df_mod)

    @pytest.mark.parametrize("data_df,schema", DATA_PARAMS)
    def test_read_non_existent_rows(
        self,
        store_maker: TableStoreMaker,
        data_df: pd.DataFrame,
        schema: DataSchema,
    ) -> None:
        store = store_maker(schema)

        if not store.caps.supports_read_nonexistent_rows:
            raise pytest.skip("Store does not support read_nonexistent_rows")

        test_df_to_store = data_df.drop(range(1, 5))

        store.insert_rows(test_df_to_store)

        assert_df_equal(
            store.read_rows(data_to_index(data_df, store.primary_keys)),
            test_df_to_store,
            index_cols=store.primary_keys,
        )

    @pytest.mark.parametrize("data_df,schema", DATA_PARAMS)
    def test_read_empty_df(
        self,
        store_maker: TableStoreMaker,
        data_df: pd.DataFrame,
        schema: DataSchema,
    ) -> None:
        store = store_maker(schema)
        store.insert_rows(data_df)

        df_empty = pd.DataFrame()

        df_result = store.read_rows(cast(IndexDF, df_empty))
        assert df_result.empty
        assert all(col in df_result.columns for col in store.primary_keys)

    @pytest.mark.parametrize("data_df,schema", DATA_PARAMS)
    def test_insert_empty_df(
        self,
        store_maker: TableStoreMaker,
        data_df: pd.DataFrame,
        schema: DataSchema,
    ) -> None:
        store = store_maker(schema)

        if not store.caps.supports_read_all_rows:
            raise pytest.skip("Store does not support read_all_rows")

        df_empty = pd.DataFrame()
        store.insert_rows(df_empty)

        df_result = store.read_rows()
        assert df_result.empty
        assert all(col in df_result.columns for col in store.primary_keys)

    @pytest.mark.parametrize("data_df,schema", DATA_PARAMS)
    def test_update_empty_df(
        self,
        store_maker: TableStoreMaker,
        data_df: pd.DataFrame,
        schema: DataSchema,
    ) -> None:
        store = store_maker(schema)

        if not store.caps.supports_read_all_rows:
            raise pytest.skip("Store does not support read_all_rows")

        df_empty = pd.DataFrame()
        store.update_rows(df_empty)

        df_result = store.read_rows()
        assert df_result.empty
        assert all(col in df_result.columns for col in store.primary_keys)

    @pytest.mark.parametrize("data_df,schema", DATA_PARAMS)
    def test_partial_update_rows(
        self,
        store_maker: TableStoreMaker,
        data_df: pd.DataFrame,
        schema: DataSchema,
    ) -> None:
        store = store_maker(schema)
        store.insert_rows(data_df)

        assert_ts_contains(store, data_df)

        test_df_mod = data_df.copy()
        test_df_mod.loc[50:, "price"] = test_df_mod.loc[50:, "price"] + 1

        store.update_rows(test_df_mod.loc[50:])

        assert_ts_contains(store, test_df_mod)

    @pytest.mark.parametrize("data_df,schema", DATA_PARAMS)
    def test_full_update_rows(
        self,
        store_maker: TableStoreMaker,
        data_df: pd.DataFrame,
        schema: DataSchema,
    ) -> None:
        store = store_maker(schema)
        store.insert_rows(data_df)

        assert_ts_contains(store, data_df)

        data_df_mod = data_df.copy()
        data_df_mod.loc[:, "price"] = data_df_mod.loc[:, "price"] + 1

        store.update_rows(data_df_mod)

        assert_ts_contains(store, data_df_mod)
