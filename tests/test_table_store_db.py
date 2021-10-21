import pandas as pd

from sqlalchemy.sql.schema import Column
from sqlalchemy.sql.sqltypes import Integer, String

from datapipe.store.database import TableStoreDB, ConstIdx

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
    Column("name", String(100))
]


def test_for_inserting(dbconn) -> None:
    store1 = TableStoreDB(
        dbconn,
        'feed_data',
        SQL_SHEMA,
        const_idx=[
            ConstIdx(
                column=Column("pipe_id", Integer()),
                value=1
            )
        ]

    )

    store2 = TableStoreDB(
        dbconn,
        'feed_data',
        SQL_SHEMA,
        const_idx=[
            ConstIdx(
                column=Column("pipe_id", Integer()),
                value=2
            )
        ]
    )

    df1 = pd.DataFrame(data=FEED1).set_index('id')
    df2 = pd.DataFrame(data=FEED2).set_index('id')

    store1.insert_rows(df1)
    stored_df1_index = sorted(store1.read_rows().index)

    assert(list(df1.index) == list(stored_df1_index))

    store2.insert_rows(df2)
    stored_df1_index = sorted(store1.read_rows().index)
    stored_df2_index = sorted(store2.read_rows().index)

    assert(list(df1.index) == list(stored_df1_index))
    assert(list(df2.index) == list(stored_df2_index))


def test_insert_identical_rows_twice_and_read_rows(dbconn) -> None:
    store1 = TableStoreDB(
        dbconn,
        'feed_data',
        SQL_SHEMA,
        const_idx=[
            ConstIdx(
                column=Column("pipe_id", Integer()),
                value=1
            )
        ]

    )

    df1 = pd.DataFrame(data=FEED1).set_index('id')

    store1.insert_rows(df1)

    mod_df1 = df1.copy()
    mod_df1.head(3)['name'] = mod_df1.head(3)['name'].map(str.capitalize)

    store1.insert_rows(mod_df1.head(3))

    stored_df1 = store1.read_rows().sort_index()

    assert(mod_df1.equals(stored_df1))


def test_for_updating(dbconn) -> None:
    store1 = TableStoreDB(
        dbconn,
        'feed_data',
        SQL_SHEMA,
        const_idx=[
            ConstIdx(
                column=Column("pipe_id", Integer()),
                value=1
            )
        ]

    )

    store2 = TableStoreDB(
        dbconn,
        'feed_data',
        SQL_SHEMA,
        const_idx=[
            ConstIdx(
                column=Column("pipe_id", Integer()),
                value=2
            )
        ]
    )

    df1 = pd.DataFrame(data=FEED1).set_index('id')
    df2 = pd.DataFrame(data=FEED2).set_index('id')
    df3 = pd.DataFrame(data=FEED3).set_index('id')

    store1.insert_rows(df1)
    store2.insert_rows(df2)

    store1.update_rows(df3)

    stored_df1 = store1.read_rows().sort_index()
    stored_df2_index = sorted(store2.read_rows().index)

    df1.update(df3)

    assert(df1.equals(stored_df1))
    assert(list(df2.index) == list(stored_df2_index))


def test_for_deliting(dbconn) -> None:
    store1 = TableStoreDB(
        dbconn,
        'feed_data',
        SQL_SHEMA,
        const_idx=[
            ConstIdx(
                column=Column("pipe_id", Integer()),
                value=1
            )
        ]

    )

    store2 = TableStoreDB(
        dbconn,
        'feed_data',
        SQL_SHEMA,
        const_idx=[
            ConstIdx(
                column=Column("pipe_id", Integer()),
                value=2
            )
        ]
    )

    df1 = pd.DataFrame(data=FEED1).set_index('id')
    df2 = pd.DataFrame(data=FEED2).set_index('id')

    store1.insert_rows(df1)
    store2.insert_rows(df2)

    df1 = df1.drop('1')
    del_index = pd.Index(["1"])

    store1.delete_rows(del_index)

    stored_df1 = store1.read_rows()
    stored_df2 = store2.read_rows()

    assert(list(df1.index) == list(stored_df1.index))
    assert(list(df2.index) == list(stored_df2.index))
