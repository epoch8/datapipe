
import pandas as pd

from sqlalchemy import Column
from sqlalchemy.sql.sqltypes import JSON, Integer

from datapipe.compute import ComputeInput, ComputeOutput
from datapipe.datatable import DataStore

from datapipe.step.batch_generate import do_batch_generate
from datapipe.step.chain_transform import ChainTransformStep
from datapipe.store.database import TableStoreDB

from datapipe.tests.util import assert_datatable_equal, assert_df_equal


TEST_SCHEMA: list[Column] = [
    Column("id", Integer, primary_key=True),
    Column("a", Integer),
]


TEST_DF = pd.DataFrame(
    {
        "id": range(10),
        "a": range(10),
    },
)

TEST_DF_SUM = pd.DataFrame(
    {
        "id": range(10),
        "a": [sum(range(i + 1)) for i in range(10)],
    },
)


def rank_func(**kwargs):
    return kwargs["id"]


def test_empty_chain_run(dbconn) -> None:
    ds = DataStore(dbconn, create_meta_table=True)
    
    tbl = ds.create_table("tbl", table_store=TableStoreDB(dbconn, "tbl_data", TEST_SCHEMA, True))
    tbl_sum = ds.create_table("tbl_sum", table_store=TableStoreDB(dbconn, "tbl_sum_data", TEST_SCHEMA, True))

    def sum_func(df, prev_df):
        return df

    step = ChainTransformStep(
        ds=ds,
        name="test",
        func=sum_func,
        rank_func=rank_func,
        input_dts=[ComputeInput(dt=tbl, join_type="full")],
        previous_dts=[ComputeInput(dt=tbl_sum, join_type="full")],
        output_dts=[ComputeOutput(dt=tbl_sum)],
    )

    step.run_full(ds)

    assert_datatable_equal(tbl, pd.DataFrame(columns=TEST_DF.columns))
    assert_datatable_equal(tbl_sum, pd.DataFrame(columns=TEST_DF_SUM.columns))


def test_chain_process(dbconn) -> None:
    ds = DataStore(dbconn, create_meta_table=True)

    tbl = ds.create_table("tbl", table_store=TableStoreDB(dbconn, "tbl_data", TEST_SCHEMA, True))
    tbl_sum = ds.create_table("tbl_sum", table_store=TableStoreDB(dbconn, "tbl_sum_data", TEST_SCHEMA, True))

    tbl.store_chunk(TEST_DF)

    def sum_func(df, prev_df):
        df = pd.concat([prev_df, df])

        df["a"] = df["a"].cumsum()

        return df[~df['id'].isin(prev_df["id"])]

    step = ChainTransformStep(
        ds=ds,
        name="test",
        func=sum_func,
        rank_func=rank_func,
        input_dts=[ComputeInput(dt=tbl, join_type="full")],
        previous_dts=[ComputeInput(dt=tbl_sum, join_type="full")],
        output_dts=[ComputeOutput(dt=tbl_sum)],
    )

    step.run_full(ds)

    assert_datatable_equal(tbl, TEST_DF)
    assert_datatable_equal(tbl_sum, TEST_DF_SUM)

    # Check this not delete the tables
    step.run_full(ds)

    assert_datatable_equal(tbl, TEST_DF)
    assert_datatable_equal(tbl_sum, TEST_DF_SUM)


"""
мулти трансформ
mylti transform with hakf traansform keys 
chunk_sizw
window_size
order
modify value
delete value
increase range
"""