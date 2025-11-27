from typing import List

import pandas as pd
from sqlalchemy import Column, Integer

from datapipe.compute import ComputeInput
from datapipe.datatable import DataStore
from datapipe.step.batch_transform import BatchTransformStep
from datapipe.store.database import TableStoreDB

TEST_SCHEMA: List[Column] = [
    Column("id", Integer, primary_key=True),
    Column("a", Integer),
]

TEST_DF = pd.DataFrame(
    {
        "id": range(10),
        "a": range(10),
    },
)

TEST_DF_INC1 = TEST_DF.assign(a=lambda df: df["a"] + 1)


def test_inc_process_proc_no_change(dbconn) -> None:
    ds = DataStore(dbconn, create_meta_table=True)

    tbl1 = ds.create_table("tbl1", table_store=TableStoreDB(dbconn, "tbl1_data", TEST_SCHEMA, True))
    tbl2 = ds.create_table("tbl2", table_store=TableStoreDB(dbconn, "tbl2_data", TEST_SCHEMA, True))

    def id_func(df):
        return TEST_DF

    step = BatchTransformStep(
        ds=ds,
        name="step",
        func=id_func,
        input_dts=[
            ComputeInput(dt=tbl1, join_type="full"),
        ],
        output_dts=[tbl2],
    )

    tbl2.store_chunk(TEST_DF)
    tbl1.store_chunk(TEST_DF)

    count, idx_gen = step.get_full_process_ids(ds)
    idx_dfs = list(idx_gen)
    idx_len = len(pd.concat(idx_dfs)) if len(idx_dfs) > 0 else 0

    assert idx_len == len(TEST_DF)

    step.run_full(ds)

    count, idx_gen = step.get_full_process_ids(ds)
    idx_dfs = list(idx_gen)
    idx_len = len(pd.concat(idx_dfs)) if len(idx_dfs) > 0 else 0

    assert idx_len == 0

    tbl1.store_chunk(TEST_DF_INC1)

    count, idx_gen = step.get_full_process_ids(ds)
    idx_dfs = list(idx_gen)
    idx_len = len(pd.concat(idx_dfs)) if len(idx_dfs) > 0 else 0

    assert idx_len == len(TEST_DF)

    step.run_full(ds)

    count, idx_gen = step.get_full_process_ids(ds)
    idx_dfs = list(idx_gen)
    idx_len = len(pd.concat(idx_dfs)) if len(idx_dfs) > 0 else 0

    assert idx_len == 0


def test_aux_input(dbconn) -> None:
    ds = DataStore(dbconn, create_meta_table=True)

    tbl_items1 = ds.create_table(
        "tbl_items1",
        table_store=TableStoreDB(
            dbconn,
            "tbl_items1_data",
            [
                Column("id", Integer, primary_key=True),
                Column("a", Integer),
            ],
            True,
        ),
    )
    tbl_items1.store_chunk(TEST_DF)

    tbl_items2 = ds.create_table(
        "tbl_items2",
        table_store=TableStoreDB(
            dbconn,
            "tbl_items2_data",
            [
                Column("id", Integer, primary_key=True),
                Column("id2", Integer, primary_key=True),
                Column("a", Integer),
            ],
            True,
        ),
    )
    tbl_items2.store_chunk(
        pd.DataFrame(
            {
                "id": [i for j in range(10) for i in range(10)],
                "id2": [i for i in range(10) for j in range(10)],
                "a": range(100),
            }
        )
    )

    tbl_aux = ds.create_table(
        "tbl_aux",
        table_store=TableStoreDB(
            dbconn,
            "tbl_aux_data",
            [
                Column("aux_id", Integer, primary_key=True),
                Column("a", Integer),
            ],
            True,
        ),
    )
    tbl_aux.store_chunk(pd.DataFrame({"aux_id": range(10), "a": range(10)}))

    tbl_out = ds.create_table(
        "tbl_out",
        table_store=TableStoreDB(
            dbconn,
            "tbl_out_data",
            [
                Column("id", Integer, primary_key=True),
                Column("a", Integer),
            ],
            True,
        ),
    )

    step = BatchTransformStep(
        ds=ds,
        name="step",
        func=lambda items1, items2, aux: items1,
        input_dts=[
            ComputeInput(dt=tbl_items1, join_type="full"),
            ComputeInput(dt=tbl_items2, join_type="full"),
            ComputeInput(dt=tbl_aux, join_type="full"),
        ],
        output_dts=[tbl_out],
        transform_keys=["id"],
    )

    idx_count, idx_gen = step.get_full_process_ids(ds)

    ids = pd.concat(list(idx_gen))

    assert len(ids) == len(ids.drop_duplicates())
