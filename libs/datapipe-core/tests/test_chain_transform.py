
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

TEST_SCHEMA_2: list[Column] = [
    Column("id_1", Integer, primary_key=True),
    Column("id_2", Integer, primary_key=True),
    Column("a", Integer),
]

TEST_SCHEMA_3: list[Column] = [
    Column("id_1", Integer, primary_key=True),
    Column("a", Integer),
]



TEST_DF = pd.DataFrame(
    {
        "id": range(10),
        "a": range(10),
    },
)

TEST_DF_2 = pd.DataFrame(
    {
        "id": range(10),
        "a": range(10, 20),
    },
)

TEST_DF_3 = pd.DataFrame(
    {
        "id_1": 10 * [1] + 10 * [2],
        "id_2": 2 * list(range(10)),
        "a": range(20),
    },
)

TEST_DF_SUM = pd.DataFrame(
    {
        "id": range(10),
        "a": [sum(range(i + 1)) for i in range(10)],
    },
)

TEST_DF_SUM_2 = pd.DataFrame(
    {
        "id": range(10),
        "a": [
            sum([2 * j + 10 for j in range(i + 1)]) 
            for i in range(10)
        ],
    },
)


def rank_func(**kwargs):
    return kwargs["id"]

def rank_func_multi(**kwargs):
    return kwargs["id_1"] * 10 + kwargs["id_2"]


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
        previous_dts=[ComputeInput(dt=tbl_sum)],
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


def test_chain_process_chunk_size(dbconn) -> None:
    ds = DataStore(dbconn, create_meta_table=True)

    tbl = ds.create_table("tbl", table_store=TableStoreDB(dbconn, "tbl_data", TEST_SCHEMA, True))
    tbl_sum = ds.create_table("tbl_sum", table_store=TableStoreDB(dbconn, "tbl_sum_data", TEST_SCHEMA, True))

    tbl.store_chunk(TEST_DF)

    def sum_func(df, prev_df):
        assert len(df) == 2

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
        chunk_size=2
    )

    step.run_full(ds)

    assert_datatable_equal(tbl, TEST_DF)
    assert_datatable_equal(tbl_sum, TEST_DF_SUM)


def test_chain_process_modify(dbconn) -> None:
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
        chunk_size=2
    )

    step.run_full(ds)

    assert_datatable_equal(tbl, TEST_DF)
    assert_datatable_equal(tbl_sum, TEST_DF_SUM)

    new_df = TEST_DF.copy()
    new_df_sum = TEST_DF_SUM.copy()

    new_df.loc[new_df['id'] == 5, 'a'] += 1
    new_df_sum.loc[new_df_sum['id'] >= 5, 'a'] += 1

    tbl.store_chunk(new_df)
    step.run_full(ds)

    assert_datatable_equal(tbl, new_df)
    assert_datatable_equal(tbl_sum, new_df_sum)


def test_chain_process_delete(dbconn) -> None:
    ds = DataStore(dbconn, create_meta_table=True)

    tbl = ds.create_table("tbl", table_store=TableStoreDB(dbconn, "tbl_data", TEST_SCHEMA, True))
    tbl_sum1 = ds.create_table("tbl_sum1", table_store=TableStoreDB(dbconn, "tbl1_sum_data", TEST_SCHEMA, True))
    tbl_sum2 = ds.create_table("tbl_sum2", table_store=TableStoreDB(dbconn, "tbl2_sum_data", TEST_SCHEMA, True))

    tbl.store_chunk(TEST_DF)

    def sum_func(df, prev_df1, prev_df2, idx):
        df1 = idx.merge(df, how="left", on="id").fillna(0)

        df1 = pd.concat([prev_df2, df1])
        df2 = pd.concat([prev_df2, df])

        df1["a"] = df1["a"].cumsum()
        df2["a"] = df2["a"].cumsum()

        return (
            df1[~df1['id'].isin(prev_df1["id"])],
            df2[~df2['id'].isin(prev_df2["id"])]
        )

    step = ChainTransformStep(
        ds=ds,
        name="test",
        func=sum_func,
        rank_func=rank_func,
        input_dts=[ComputeInput(dt=tbl, join_type="full")],
        previous_dts=[
            ComputeInput(dt=tbl_sum1, join_type="full"),
            ComputeInput(dt=tbl_sum2, join_type="full"),
        ],
        output_dts=[
            ComputeOutput(dt=tbl_sum1),
            ComputeOutput(dt=tbl_sum2)
        ],
        chunk_size=2
    )

    step.run_full(ds)

    assert_datatable_equal(tbl, TEST_DF)
    assert_datatable_equal(tbl_sum1, TEST_DF_SUM)
    assert_datatable_equal(tbl_sum2, TEST_DF_SUM)

    new_df = TEST_DF.copy()
    new_df_sum = TEST_DF_SUM.copy()

    new_df = new_df[new_df["id"] != 5]
    new_df_sum.loc[new_df_sum['id'] >= 5, 'a'] -= 5
    new_df_sum2 = new_df_sum[new_df_sum["id"] != 5]

    # new_df_sum - check delete with safe range in output table
    # new_df_sum2 - check delete without safe range in output table

    tbl.store_chunk(new_df, TEST_DF[["id"]])
    step.run_full(ds)

    assert_datatable_equal(tbl, new_df)
    assert_datatable_equal(tbl_sum1, new_df_sum)
    assert_datatable_equal(tbl_sum2, new_df_sum2)


def test_chain_process_many_several_inputs(dbconn) -> None:
    ds = DataStore(dbconn, create_meta_table=True)

    tbl1 = ds.create_table("tbl1", table_store=TableStoreDB(dbconn, "tbl1_data", TEST_SCHEMA, True))
    tbl2 = ds.create_table("tbl2", table_store=TableStoreDB(dbconn, "tbl2_data", TEST_SCHEMA, True))
    tbl_sum = ds.create_table("tbl_sum", table_store=TableStoreDB(dbconn, "tbl_sum_data", TEST_SCHEMA, True))

    tbl1.store_chunk(TEST_DF)
    tbl2.store_chunk(TEST_DF_2)

    def sum_func(df1, df2, prev_df):
        df = df1.merge(df2, how="outer", on="id", suffixes=["_1", "_2"]).fillna(0)
        df["a"] = df.apply(lambda x: x["a_1"] + x["a_2"], axis=1)

        df = pd.concat([prev_df, df[TEST_DF.columns]])
        df["a"] = df["a"].cumsum()

        return df[~df['id'].isin(prev_df["id"])]

    step = ChainTransformStep(
        ds=ds,
        name="test",
        func=sum_func,
        rank_func=rank_func,
        input_dts=[
            ComputeInput(dt=tbl1, join_type="full"),
            ComputeInput(dt=tbl2, join_type="full")
        ],
        previous_dts=[ComputeInput(dt=tbl_sum)],
        output_dts=[ComputeOutput(dt=tbl_sum)],
    )

    step.run_full(ds)

    assert_datatable_equal(tbl1, TEST_DF)
    assert_datatable_equal(tbl2, TEST_DF_2)
    assert_datatable_equal(tbl_sum, TEST_DF_SUM_2)


def test_chain_process_many_several_outputs(dbconn) -> None:
    ds = DataStore(dbconn, create_meta_table=True)

    tbl = ds.create_table("tbl", table_store=TableStoreDB(dbconn, "tbl_data", TEST_SCHEMA, True))
    tbl_sum1 = ds.create_table("tbl_sum1", table_store=TableStoreDB(dbconn, "tbl_sum1_data", TEST_SCHEMA, True))
    tbl_sum2 = ds.create_table("tbl_sum2", table_store=TableStoreDB(dbconn, "tbl_sum2_data", TEST_SCHEMA, True))

    tbl.store_chunk(TEST_DF)

    def sum_func(df, prev_df1, prev_df2, idx):
        df["a_2"] = df["a"].apply(lambda x: 2 * x + 10)

        df1 = pd.concat([prev_df1, df[["id", "a"]]].copy())
        df2 = pd.concat([prev_df2, df[["id", "a_2"]].rename(columns={'a_2': 'a'})])

        df1["a"] = df1["a"].cumsum()
        df2["a"] = df2["a"].cumsum()

        return (
            df1[df1['id'].isin(idx["id"])],
            df2[df2['id'].isin(idx["id"])]
        )

    step = ChainTransformStep(
        ds=ds,
        name="test",
        func=sum_func,
        rank_func=rank_func,
        input_dts=[ComputeInput(dt=tbl, join_type="full")],
        previous_dts=[
            ComputeInput(dt=tbl_sum1),
            ComputeInput(dt=tbl_sum2),
        ],
        output_dts=[
            ComputeOutput(dt=tbl_sum1),
            ComputeOutput(dt=tbl_sum2)
        ],
    )

    step.run_full(ds)

    assert_datatable_equal(tbl, TEST_DF)
    assert_datatable_equal(tbl_sum1, TEST_DF_SUM)
    assert_datatable_equal(tbl_sum2, TEST_DF_SUM_2)


def test_chain_process_many_transform_keys(dbconn) -> None:
    ds = DataStore(dbconn, create_meta_table=True)

    tbl1 = ds.create_table("tbl1", table_store=TableStoreDB(dbconn, "tbl1_data", TEST_SCHEMA_2, True))
    tbl2 = ds.create_table("tbl2", table_store=TableStoreDB(dbconn, "tbl2_data", TEST_SCHEMA_2, True))
    tbl_sum = ds.create_table("tbl_sum", table_store=TableStoreDB(dbconn, "tbl_sum_data", TEST_SCHEMA_2, True))

    tbl2_df = TEST_DF_3.copy()
    tbl2_df = tbl2_df[tbl2_df["id_2"] >= 5]
    tbl2_df["a"] += 1

    tbl1.store_chunk(TEST_DF_3)
    tbl2.store_chunk(tbl2_df)

    def sum_func(df1, df2, prev_df, idx):
        df = df1.merge(df2, how="outer", on=["id_1", "id_2"], suffixes=["_1", "_2"]).fillna(0)
        df["a"] = df.apply(lambda x: x["a_1"] + x["a_2"], axis=1)

        df = pd.concat([prev_df, df[TEST_DF_3.columns]])
        df["a"] = df["a"].cumsum()

        merged_df = idx.merge(df, on=["id_1", "id_2"], how='left', indicator=True)

        return merged_df[merged_df['_merge'] == 'both'].drop(columns=['_merge'])
    
    step = ChainTransformStep(
        ds=ds,
        name="test",
        func=sum_func,
        rank_func=rank_func_multi,
        input_dts=[
            ComputeInput(dt=tbl1, join_type="full"),
            ComputeInput(dt=tbl2, join_type="full")
        ],
        previous_dts=[ComputeInput(dt=tbl_sum)],
        output_dts=[ComputeOutput(dt=tbl_sum)],
    )

    step.run_full(ds)

    tbl_sum_df = TEST_DF_3.merge(tbl2_df, how="outer", on=["id_1", "id_2"], suffixes=["_x", "_y"]).fillna(0)
    tbl_sum_df["a"] = tbl_sum_df.apply(lambda x: x["a_x"] + x["a_y"], axis=1).cumsum()
    tbl_sum_df = tbl_sum_df[TEST_DF_3.columns]

    assert_datatable_equal(tbl1, TEST_DF_3)
    assert_datatable_equal(tbl2, tbl2_df)
    assert_datatable_equal(tbl_sum, tbl_sum_df)


def test_transform_keys_with_incorrect_primary_keys_in_table(dbconn) -> None:
    ds = DataStore(dbconn, create_meta_table=True)

    tbl1 = ds.create_table("tbl1", table_store=TableStoreDB(dbconn, "tbl1_data", TEST_SCHEMA_2, True))
    tbl2 = ds.create_table("tbl2", table_store=TableStoreDB(dbconn, "tbl2_data", TEST_SCHEMA_2, True))
    tbl3 = ds.create_table("tbl3", table_store=TableStoreDB(dbconn, "tbl3_data", TEST_SCHEMA_3, True))
    tbl_sum = ds.create_table("tbl_sum", table_store=TableStoreDB(dbconn, "tbl_sum_data", TEST_SCHEMA_2, True))

    tbl2_df = TEST_DF_3.copy()
    tbl2_df = tbl2_df[tbl2_df["id_2"] >= 5]
    tbl2_df["a"] += 1

    tbl3_df = TEST_DF.copy().rename(columns={"id": "id_1"})
    tbl3_df = tbl3_df[tbl3_df["id_1"] <= 3]
    tbl3_df["a"] += 1

    tbl1.store_chunk(TEST_DF_3)
    tbl2.store_chunk(tbl2_df)
    tbl3.store_chunk(tbl3_df)

    def sum_func(df1, df2, df3, prev_df, idx):
        df = df1.merge(df2, how="outer", on=["id_1", "id_2"], suffixes=["_1", "_2"]).fillna(0)
        df = df.merge(df3, how="outer", on=["id_1"]).dropna(subset=["id_1", "id_2"]).fillna(0)
        df["a"] = df.apply(lambda x: x["a_1"] + x["a_2"] + x["a"], axis=1)

        df = pd.concat([prev_df, df[TEST_DF_3.columns]])
        df["a"] = df["a"].cumsum()

        merged_df = idx.merge(df, on=["id_1", "id_2"], how='left', indicator=True)

        return merged_df[merged_df['_merge'] == 'both'].drop(columns=['_merge'])
    
    step = ChainTransformStep(
        ds=ds,
        name="test",
        func=sum_func,
        rank_func=rank_func_multi,
        input_dts=[
            ComputeInput(dt=tbl1, join_type="full"),
            ComputeInput(dt=tbl2, join_type="full"),
            ComputeInput(dt=tbl3, join_type="full")
        ],
        previous_dts=[ComputeInput(dt=tbl_sum)],
        output_dts=[ComputeOutput(dt=tbl_sum)],
        transform_keys=["id_1", "id_2"]
    )

    step.run_full(ds)


    tbl_sum_df = TEST_DF_3.merge(tbl2_df, how="outer", on=["id_1", "id_2"], suffixes=["_x", "_y"]).fillna(0)
    tbl_sum_df = tbl_sum_df.merge(tbl3_df, how="outer", on=["id_1"]).dropna(subset=["id_1", "id_2"]).fillna(0)

    tbl_sum_df["a"] = tbl_sum_df.apply(lambda x: x["a_x"] + x["a_y"] + x["a"], axis=1).cumsum()
    tbl_sum_df = tbl_sum_df[TEST_DF_3.columns]

    assert_datatable_equal(tbl1, TEST_DF_3)
    assert_datatable_equal(tbl2, tbl2_df)
    assert_datatable_equal(tbl3, tbl3_df)
    assert_datatable_equal(tbl_sum, tbl_sum_df)


def test_chain_process_chunk_size(dbconn) -> None:
    ds = DataStore(dbconn, create_meta_table=True)

    tbl = ds.create_table("tbl", table_store=TableStoreDB(dbconn, "tbl_data", TEST_SCHEMA, True))
    tbl_sum = ds.create_table("tbl_sum", table_store=TableStoreDB(dbconn, "tbl_sum_data", TEST_SCHEMA, True))

    tbl.store_chunk(TEST_DF)

    def sum_func(df, prev_df):
        df = pd.concat([prev_df, df])

        df["a"] = df["a"].rolling(window=3, min_periods=1).sum()

        return df[~df['id'].isin(prev_df["id"])]

    step = ChainTransformStep(
        ds=ds,
        name="test",
        func=sum_func,
        rank_func=rank_func,
        input_dts=[ComputeInput(dt=tbl, join_type="full")],
        previous_dts=[ComputeInput(dt=tbl)],
        output_dts=[ComputeOutput(dt=tbl_sum)],
        window_size=2
    )

    step.run_full(ds)

    tbl_window_df = TEST_DF.copy()
    tbl_window_df["a"] = tbl_window_df["a"].rolling(window=3, min_periods=1).sum()

    assert_datatable_equal(tbl, TEST_DF)
    assert_datatable_equal(tbl_sum, tbl_window_df)


def test_chain_order(dbconn) -> None:
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
        order="desc"
    )

    df_sum = TEST_DF.copy().sort_values('a', ascending=False)
    df_sum['a'] = df_sum['a'].cumsum()

    step.run_full(ds)

    assert_datatable_equal(tbl, TEST_DF)
    assert_datatable_equal(tbl_sum, df_sum)

    # Check this not delete the tables
    step.run_full(ds)

    assert_datatable_equal(tbl, TEST_DF)
    assert_datatable_equal(tbl_sum, df_sum)


def test_chain_increase_range(dbconn) -> None:
    ds = DataStore(dbconn, create_meta_table=True)

    tbl = ds.create_table("tbl", table_store=TableStoreDB(dbconn, "tbl_data", TEST_SCHEMA, True))
    tbl_sum = ds.create_table("tbl_sum", table_store=TableStoreDB(dbconn, "tbl_sum_data", TEST_SCHEMA, True))

    tbl_df = TEST_DF.copy()
    tbl_df = pd.concat([tbl_df[tbl_df["id"] < 3], tbl_df[tbl_df["id"] > 7]])

    tbl.store_chunk(tbl_df)

    def sum_func(df, prev_df, idx):
        df = idx.merge(df, how="left", on="id")
        df = pd.concat([prev_df, df])
        
        min_id = int(df["id"].min())
        max_id = int(df["id"].max())
        df_idx = pd.DataFrame({"id": range(min_id, max_id + 1)})
        df = df_idx.merge(df, how="left", on="id").fillna(0)

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

    df_range = tbl_df.copy()
    min_id = int(df_range["id"].min())
    max_id = int(df_range["id"].max())
    df_index = pd.DataFrame({"id": range(min_id, max_id + 1)})
    df_range = df_index.merge(df_range, how="left", on="id").fillna(0)

    df_range["a"] = df_range["a"].cumsum()

    step.run_full(ds)

    assert_datatable_equal(tbl, tbl_df)
    assert_datatable_equal(tbl_sum, df_range)

    # Check full process idx
    count, idx = step.get_full_process_ids(ds)

    assert count == 0

    # Check this not delete the incresed data
    step.run_full(ds)

    assert_datatable_equal(tbl, tbl_df)
    assert_datatable_equal(tbl_sum, df_range)