import cloudpickle
import pandas as pd
from sqlalchemy import Column
from sqlalchemy.sql.sqltypes import Integer, JSON

from datapipe.store.database import DBConn, TableStoreDB
from datapipe.datatable import DataStore
from datapipe.types import data_to_index

from .util import assert_df_equal, assert_datatable_equal


TEST_SCHEMA = [
    Column('id', Integer, primary_key=True),
    Column('a', Integer),
]

TEST_SCHEMA_OTM = [
    Column('id', Integer, primary_key=True),
    Column('a', JSON),
]

TEST_SCHEMA_OTM2 = [
    Column('id', Integer, primary_key=True),
    Column('a', Integer, primary_key=True)
]

TEST_SCHEMA_OTM3 = [
    Column('a', Integer, primary_key=True),
    Column('b', Integer, primary_key=True),
    Column('ids', JSON)
]

TEST_DF = pd.DataFrame(
    {
        'id': range(10),
        'a': range(10),
    },
)

TEST_OTM_DF = pd.DataFrame(
    {
        'id': range(10),
        'a': [[j for j in range(i)] for i in range(10)],
    },
)


TEST_DF_INC1 = TEST_DF.assign(a=lambda df: df['a'] + 1)
TEST_DF_INC2 = TEST_DF.assign(a=lambda df: df['a'] + 2)
TEST_DF_INC3 = TEST_DF.assign(a=lambda df: df['a'] + 3)


def yield_df(data):
    def f(*args, **kwargs):
        yield pd.DataFrame.from_records(data, columns=['id', 'a']).set_index('id')

    return f


def test_cloudpickle(dbconn) -> None:
    ds = DataStore(meta_dbconn=dbconn)

    tbl = ds.create_table(
        name='test',
        table_store=TableStoreDB(dbconn, 'test_data', TEST_SCHEMA, True)
    )

    dump = cloudpickle.dumps([ds, tbl])

    _, tbl_desrl = cloudpickle.loads(dump)

    dbconn_a = tbl.meta_dbconn
    dbconn_b: DBConn = tbl_desrl.meta_dbconn

    assert (
        (dbconn_a.connstr, dbconn_a.schema, dbconn_a.supports_update_from)
        ==
        (dbconn_b.connstr, dbconn_b.schema, dbconn_b.supports_update_from)
    )


def test_simple(dbconn) -> None:
    ds = DataStore(dbconn)

    tbl = ds.create_table(
        'test',
        table_store=TableStoreDB(dbconn, 'test_data', TEST_SCHEMA, True)
    )

    tbl.store_chunk(TEST_DF)

    assert_datatable_equal(tbl, TEST_DF)


def test_store_less_values(dbconn) -> None:
    ds = DataStore(dbconn)

    tbl = ds.create_table(
        'test',
        table_store=TableStoreDB(dbconn, 'test_data', TEST_SCHEMA, True)
    )

    tbl.store_chunk(TEST_DF)
    assert_datatable_equal(tbl, TEST_DF)

    tbl.store_chunk(TEST_DF[:5], processed_idx=data_to_index(TEST_DF, tbl.primary_keys))
    assert_datatable_equal(tbl, TEST_DF[:5])


def test_get_process_ids(dbconn) -> None:
    ds = DataStore(dbconn)

    tbl1 = ds.create_table(
        'tbl1',
        table_store=TableStoreDB(dbconn, 'tbl1_data', TEST_SCHEMA, True)
    )
    tbl2 = ds.create_table(
        'tbl2',
        table_store=TableStoreDB(dbconn, 'tbl2_data', TEST_SCHEMA, True)
    )

    tbl1.store_chunk(TEST_DF)

    count, idx_dfs = ds.get_process_ids([tbl1], [tbl2])
    idx = pd.concat(list(idx_dfs))

    assert(sorted(list(idx.index)) == list(TEST_DF.index))

    tbl2.store_chunk(tbl1.get_data())

    upd_df = TEST_DF[:5].copy()
    upd_df['a'] += 1

    tbl1.store_chunk(upd_df)

    count, idx_dfs = ds.get_process_ids([tbl1], [tbl2])
    idx = pd.concat(list(idx_dfs))

    assert_df_equal(idx, upd_df[['id']])
