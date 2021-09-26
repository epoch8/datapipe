import pandas as pd

from sqlalchemy.sql.schema import Column
from sqlalchemy.sql.sqltypes import String

from datapipe.store.database import TableStoreDB

from .util import assert_df_equal

FEED1 = [
    {"id": "1", "name": "Product 1"},
    {"id": "2", "name": "Product 2"},
    {"id": "3", "name": "Product 3"},
    {"id": "4", "name": "Product 4"},
    {"id": "5", "name": "Product 5"},
    {"id": "6", "name": "Product 6"}
]

FEED2 = [
    {"id": "1", "name": "Product 12"},
    {"id": "2", "name": "Product 22"},
    {"id": "3", "name": "Product 32"},
    {"id": "7", "name": "Product 42"},
    {"id": "8", "name": "Product 52"},
    {"id": "9", "name": "Product 62"}
]

FEED3 = [{"id": "1", "name": "Product 3"}]

SQL_SHEMA = [
    Column("id", String(100), primary_key=True),
    Column("name", String(100))
]


def test_for_inserting(dbconn) -> None:
    store1 = TableStoreDB(
        dbconn,
        'feed_data',
        SQL_SHEMA
    )

    df1 = pd.DataFrame(data=FEED1)

    store1.insert_rows(df1)
    stored_df1 = store1.read_rows()

    assert_df_equal(df1, stored_df1)


def test_for_updating(dbconn) -> None:
    store1 = TableStoreDB(
        dbconn,
        'feed_data',
        SQL_SHEMA
    )

    df1 = pd.DataFrame(data=FEED1)
    df3 = pd.DataFrame(data=FEED3)

    store1.insert_rows(df1)
    store1.update_rows(df3)

    stored_df1 = store1.read_rows()

    df1.update(df3)

    assert_df_equal(df1, stored_df1)


def test_for_deliting(dbconn) -> None:
    store1 = TableStoreDB(
        dbconn,
        'feed_data',
        SQL_SHEMA
    )

    df1 = pd.DataFrame(data=FEED1)

    store1.insert_rows(df1)

    del_index = pd.DataFrame({"id": ["1"]})

    store1.delete_rows(del_index)

    stored_df1 = store1.read_rows()

    assert_df_equal(df1.iloc[1:], stored_df1)
