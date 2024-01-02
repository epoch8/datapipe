import cloudpickle
import numpy as np
import pandas as pd
from sqlalchemy import Column
from sqlalchemy.sql.sqltypes import JSON, DateTime, Integer

from datapipe.datatable import DataStore
from datapipe.store.database import DBConn, TableStoreDB
from datapipe.types import IndexDF, data_to_index

from .util import assert_datatable_equal, assert_df_equal

TEST_SCHEMA: list[Column] = [
    Column("id", Integer, primary_key=True),
    Column("a", Integer),
]

TEST_SCHEMA_OTM: list[Column] = [
    Column("id", Integer, primary_key=True),
    Column("a", JSON),
]

TEST_SCHEMA_OTM2: list[Column] = [
    Column("id", Integer, primary_key=True),
    Column("a", Integer, primary_key=True),
]

TEST_SCHEMA_OTM3: list[Column] = [
    Column("a", Integer, primary_key=True),
    Column("b", Integer, primary_key=True),
    Column("ids", JSON),
]

TEST_SCHEMA_NA_VALUES: list[Column] = [
    Column("id", Integer, primary_key=True),
    Column("a", Integer),
    Column("b", DateTime),
]

TEST_DF = pd.DataFrame(
    {
        "id": range(10),
        "a": range(10),
    },
)

TEST_OTM_DF = pd.DataFrame(
    {
        "id": range(10),
        "a": [[j for j in range(i)] for i in range(10)],
    },
)

TEST_NA_VALUES_DF = pd.DataFrame(
    {
        "id": pd.Series(range(10), dtype="int"),
        "a": pd.Series([np.nan] * 10, dtype="float"),
        "b": pd.Series([pd.NaT] * 10, dtype="datetime64[ns]"),
    },
)


TEST_DF_INC1 = TEST_DF.assign(a=lambda df: df["a"] + 1)
TEST_DF_INC2 = TEST_DF.assign(a=lambda df: df["a"] + 2)
TEST_DF_INC3 = TEST_DF.assign(a=lambda df: df["a"] + 3)


def yield_df(data):
    def f(*args, **kwargs):
        yield pd.DataFrame.from_records(data, columns=["id", "a"]).set_index("id")

    return f


def test_cloudpickle(dbconn) -> None:
    ds = DataStore(dbconn, create_meta_table=True)

    tbl = ds.create_table(
        name="test", table_store=TableStoreDB(dbconn, "test_data", TEST_SCHEMA, True)
    )

    dump = cloudpickle.dumps([ds, tbl])

    _, tbl_desrl = cloudpickle.loads(dump)

    dbconn_a = tbl.meta_dbconn
    dbconn_b: DBConn = tbl_desrl.meta_dbconn

    assert (dbconn_a.connstr, dbconn_a.schema, dbconn_a.supports_update_from) == (
        dbconn_b.connstr,
        dbconn_b.schema,
        dbconn_b.supports_update_from,
    )


def test_simple(dbconn) -> None:
    ds = DataStore(dbconn, create_meta_table=True)

    tbl = ds.create_table(
        "test", table_store=TableStoreDB(dbconn, "test_data", TEST_SCHEMA, True)
    )

    tbl.store_chunk(TEST_DF)

    assert_datatable_equal(tbl, TEST_DF)


def test_store_less_values(dbconn) -> None:
    ds = DataStore(dbconn, create_meta_table=True)

    tbl = ds.create_table(
        "test", table_store=TableStoreDB(dbconn, "test_data", TEST_SCHEMA, True)
    )

    tbl.store_chunk(TEST_DF)
    assert_datatable_equal(tbl, TEST_DF)

    tbl.store_chunk(TEST_DF[:5], processed_idx=data_to_index(TEST_DF, tbl.primary_keys))
    assert_datatable_equal(tbl, TEST_DF[:5])


def test_store_chunk_changelist(dbconn) -> None:
    ds = DataStore(dbconn, create_meta_table=True)

    tbl = ds.create_table(
        "tbl1", table_store=TableStoreDB(dbconn, "tbl1_data", TEST_SCHEMA, True)
    )

    tbl.store_chunk(TEST_DF)

    upd_df = TEST_DF.copy()

    upd_df.loc[1, "a"] = 10
    upd_df = pd.concat(
        [upd_df, pd.DataFrame.from_records([{"id": 10, "a": 11}])], axis="index"
    )

    proc_idx = IndexDF(upd_df[["id"]])
    idx = IndexDF(pd.DataFrame({"id": [0, 1, 10]}))

    change_idx = tbl.store_chunk(upd_df[1:], processed_idx=proc_idx)

    assert_df_equal(idx, change_idx)


def test_get_size(dbconn) -> None:
    ds = DataStore(dbconn, create_meta_table=True)

    tbl = ds.create_table(
        "tbl1", table_store=TableStoreDB(dbconn, "tbl1_data", TEST_SCHEMA, True)
    )

    tbl.store_chunk(TEST_DF)

    assert tbl.get_size() == len(TEST_DF)


def test_pandas_na_values_in_table(dbconn) -> None:
    ds = DataStore(dbconn, create_meta_table=True)

    tbl = ds.create_table(
        "tbl1",
        table_store=TableStoreDB(dbconn, "tbl1_data", TEST_SCHEMA_NA_VALUES, True),
    )

    tbl.store_chunk(TEST_NA_VALUES_DF)

    assert tbl.get_size() == len(TEST_NA_VALUES_DF)
