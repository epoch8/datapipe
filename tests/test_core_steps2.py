# Ex test_compute

# from typing import cast
# import pytest

import time

import pandas as pd
from sqlalchemy import Column, String
from sqlalchemy.sql.sqltypes import Integer

from datapipe.compute import ComputeInput
from datapipe.datatable import DataStore
from datapipe.run_config import RunConfig
from datapipe.step.batch_generate import do_batch_generate
from datapipe.step.batch_transform import BatchTransformStep
from datapipe.store.database import MetaKey, TableStoreDB
from datapipe.types import ChangeList, IndexDF

from .util import assert_datatable_equal, assert_df_equal

TEST_SCHEMA1 = [
    Column("item_id", Integer, primary_key=True),
    Column("pipeline_id", Integer, primary_key=True),
    Column("a", Integer),
]

TEST_SCHEMA2 = [
    Column("item_id", Integer, primary_key=True),
    Column("a", Integer),
]

PRODUCTS_SCHEMA = [
    Column("product_id", Integer, primary_key=True),
    Column("pipeline_id", Integer, primary_key=True),
    Column("b", Integer),
]

ITEMS_SCHEMA = [
    Column("item_id", Integer, primary_key=True),
    Column("pipeline_id", Integer, primary_key=True),
    Column("product_id", Integer, MetaKey()),
    Column("a", Integer),
]

TEST_DF1_1 = pd.DataFrame(
    {
        "item_id": range(10),
        "pipeline_id": [i // 5 for i in range(10)],
        "a": range(10),
    },
)


TEST_DF1_2 = pd.DataFrame(
    {
        "item_id": list(range(5)) * 2,
        "pipeline_id": [i // 5 for i in range(10)],
        "a": range(10),
    },
)

PRODUCTS_DF = pd.DataFrame(
    {
        "product_id": list(range(2)),
        "pipeline_id": list(range(2)),
        "b": range(10, 12),
    }
)

ITEMS_DF = pd.DataFrame(
    {
        "item_id": list(range(5)) * 2,
        "pipeline_id": list(range(2)) * 5,
        "product_id": list(range(2)) * 5,
        "a": range(10),
    }
)


def test_batch_transform(dbconn):
    ds = DataStore(dbconn, create_meta_table=True)

    tbl1 = ds.create_table(
        "tbl1", table_store=TableStoreDB(dbconn, "tbl1_data", TEST_SCHEMA1, True)
    )

    tbl2 = ds.create_table(
        "tbl2", table_store=TableStoreDB(dbconn, "tbl2_data", TEST_SCHEMA1, True)
    )

    tbl1.store_chunk(TEST_DF1_1, now=0)

    step = BatchTransformStep(
        ds=ds,
        name="test",
        func=lambda df: df,
        input_dts=[ComputeInput(dt=tbl1, join_type="full")],
        output_dts=[tbl2],
    )

    step.run_full(ds)

    meta_df = tbl2.get_metadata()

    update_ts = max(meta_df["update_ts"])
    process_ts = max(meta_df["process_ts"])

    time.sleep(0.1)

    step.run_full(ds)

    meta_df = tbl2.get_metadata()

    assert all(meta_df["update_ts"] == update_ts)
    assert all(meta_df["process_ts"] == process_ts)


def test_batch_transform_with_filter(dbconn):
    ds = DataStore(dbconn, create_meta_table=True)

    tbl1 = ds.create_table(
        "tbl1", table_store=TableStoreDB(dbconn, "tbl1_data", TEST_SCHEMA1, True)
    )

    tbl2 = ds.create_table(
        "tbl2", table_store=TableStoreDB(dbconn, "tbl2_data", TEST_SCHEMA1, True)
    )

    tbl1.store_chunk(TEST_DF1_1, now=0)

    step = BatchTransformStep(
        ds=ds,
        name="test",
        func=lambda df: df,
        input_dts=[ComputeInput(dt=tbl1, join_type="full")],
        output_dts=[tbl2],
    )
    step.run_full(
        ds,
        run_config=RunConfig(
            filters={"pipeline_id": 0},
        ),
    )

    assert_datatable_equal(tbl2, TEST_DF1_1.query("pipeline_id == 0"))


def test_batch_transform_with_filter_not_in_transform_index(dbconn):
    ds = DataStore(dbconn, create_meta_table=True)

    tbl1 = ds.create_table(
        "tbl1", table_store=TableStoreDB(dbconn, "tbl1_data", TEST_SCHEMA1, True)
    )

    tbl2 = ds.create_table(
        "tbl2", table_store=TableStoreDB(dbconn, "tbl2_data", TEST_SCHEMA2, True)
    )

    tbl1.store_chunk(TEST_DF1_2, now=0)

    step = BatchTransformStep(
        ds=ds,
        name="test",
        func=lambda df: df[["item_id", "a"]],
        input_dts=[ComputeInput(dt=tbl1, join_type="full")],
        output_dts=[tbl2],
    )

    step.run_full(
        ds,
        run_config=RunConfig(filters={"pipeline_id": 0}),
    )

    assert_datatable_equal(tbl2, TEST_DF1_2.query("pipeline_id == 0")[["item_id", "a"]])


def test_batch_transform_with_dt_on_input_and_output(dbconn):
    ds = DataStore(dbconn, create_meta_table=True)

    tbl1 = ds.create_table(
        "tbl1", table_store=TableStoreDB(dbconn, "tbl1_data", TEST_SCHEMA1, True)
    )

    tbl2 = ds.create_table(
        "tbl2", table_store=TableStoreDB(dbconn, "tbl2_data", TEST_SCHEMA1, True)
    )

    df2 = TEST_DF1_1.loc[3:8]
    df2["a"] = df2["a"].apply(lambda x: x + 10)

    tbl1.store_chunk(TEST_DF1_1, now=0)
    tbl2.store_chunk(df2, now=0)

    def update_df(df1: pd.DataFrame, df2: pd.DataFrame):
        df1 = df1.set_index(["item_id", "pipeline_id"])
        df2 = df2.set_index(["item_id", "pipeline_id"])

        df1.update(df2)

        return df1.reset_index()

    step = BatchTransformStep(
        ds=ds,
        name="test",
        func=update_df,
        input_dts=[
            ComputeInput(dt=tbl1, join_type="full"),
            ComputeInput(dt=tbl2, join_type="full"),
        ],
        output_dts=[tbl2],
    )

    step.run_full(ds)

    df_res = TEST_DF1_1.copy().set_index(["item_id", "pipeline_id"])
    df_res.update(df2.set_index(["item_id", "pipeline_id"]))
    df_res = df_res.reset_index()

    assert_datatable_equal(tbl2, df_res)


def test_batch_transform_with_fails(dbconn):
    ds = DataStore(dbconn, create_meta_table=True)

    tbl1 = ds.create_table(
        "tbl1",
        table_store=TableStoreDB(
            dbconn,
            "tbl1",
            [
                Column("id", Integer, primary_key=True),
                Column("data", String),
            ],
            create_table=True,
        ),
    )

    tbl2 = ds.create_table(
        "tbl2",
        table_store=TableStoreDB(
            dbconn,
            "tbl2",
            [
                Column("id", Integer, primary_key=True),
                Column("data", String),
            ],
            create_table=True,
        ),
    )

    context = {
        "fail": True,
    }

    def transform_func(df, context=context):
        if context["fail"]:
            raise ValueError("fail")
        return df

    step = BatchTransformStep(
        ds=ds,
        name="step1",
        func=transform_func,
        input_dts=[ComputeInput(dt=tbl1, join_type="full")],
        output_dts=[tbl2],
    )

    tbl1.store_chunk(
        pd.DataFrame(
            {
                "id": range(10),
                "data": ["a"] * 10,
            }
        )
    )

    step.run_full(ds)
    assert len(tbl2.get_data()) == 0

    context["fail"] = False

    step.run_full(ds)

    assert len(tbl2.get_data()) == 10


def test_gen_with_filter(dbconn):
    ds = DataStore(dbconn, create_meta_table=True)

    tbl = ds.create_table(
        "tbl", table_store=TableStoreDB(dbconn, "tbl_data", TEST_SCHEMA1, True)
    )

    tbl.store_chunk(TEST_DF1_1, now=0)

    def gen_func():
        yield TEST_DF1_1.query("pipeline_id == 0 and item_id == 0")

    do_batch_generate(
        func=gen_func,
        ds=ds,
        output_dts=[tbl],
        run_config=RunConfig(filters={"pipeline_id": 0}),
    )

    assert_datatable_equal(
        tbl, TEST_DF1_1.query("(pipeline_id == 0 and item_id == 0) or pipeline_id == 1")
    )


def test_transform_with_changelist(dbconn):
    ds = DataStore(dbconn, create_meta_table=True)

    tbl1 = ds.create_table(
        "tbl1", table_store=TableStoreDB(dbconn, "tbl1_data", TEST_SCHEMA1, True)
    )

    tbl2 = ds.create_table(
        "tbl2", table_store=TableStoreDB(dbconn, "tbl2_data", TEST_SCHEMA1, True)
    )

    tbl1.store_chunk(TEST_DF1_1, now=0)

    def func(df):
        return df

    step = BatchTransformStep(
        ds=ds,
        name="test",
        func=func,
        input_dts=[ComputeInput(dt=tbl1, join_type="full")],
        output_dts=[tbl2],
    )

    change_list = ChangeList()

    idx_keys = ["item_id", "pipeline_id"]
    changes_df = TEST_DF1_1.loc[[0, 1, 2]]
    changes_idx = IndexDF(changes_df[idx_keys])

    change_list.append("tbl1", changes_idx)

    next_change_list = step.run_changelist(ds, change_list)

    assert_datatable_equal(tbl2, changes_df)

    assert list(next_change_list.changes.keys()) == ["tbl2"]

    assert_df_equal(next_change_list.changes["tbl2"], changes_idx, index_cols=idx_keys)


def test_batch_transform_with_entity(dbconn):
    ds = DataStore(dbconn, create_meta_table=True)

    products = ds.create_table(
        "products",
        table_store=TableStoreDB(dbconn, "products_data", PRODUCTS_SCHEMA, True),
    )

    items = ds.create_table(
        "items", table_store=TableStoreDB(dbconn, "items_data", ITEMS_SCHEMA, True)
    )

    items2 = ds.create_table(
        "items2", table_store=TableStoreDB(dbconn, "items2_data", ITEMS_SCHEMA, True)
    )

    products.store_chunk(PRODUCTS_DF, now=0)
    items.store_chunk(ITEMS_DF, now=0)

    def update_df(products: pd.DataFrame, items: pd.DataFrame):
        merged_df = pd.merge(items, products, on=["product_id", "pipeline_id"])
        merged_df["a"] = merged_df.apply(lambda x: x["a"] + x["b"], axis=1)

        return merged_df[["item_id", "pipeline_id", "product_id", "a"]]

    step = BatchTransformStep(
        ds=ds,
        name="test",
        func=update_df,
        input_dts=[
            ComputeInput(dt=products, join_type="full"),
            ComputeInput(dt=items, join_type="full"),
        ],
        output_dts=[items2],
    )

    step.run_full(ds)

    merged_df = pd.merge(ITEMS_DF, PRODUCTS_DF, on=["product_id", "pipeline_id"])
    merged_df["a"] = merged_df.apply(lambda x: x["a"] + x["b"], axis=1)

    items2_df = merged_df[["item_id", "pipeline_id", "product_id", "a"]]

    assert_df_equal(items2.get_data(), items2_df, index_cols=["item_id", "pipeline_id"])
