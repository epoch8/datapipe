from typing import cast
import pandas as pd

from sqlalchemy import Integer
from sqlalchemy.sql.schema import Column

from datapipe.metastore import MetaTable
from datapipe.store.database import DBConn
from datapipe.types import DataDF, meta_to_index

from .util import assert_df_equal

TEST_DF = cast(DataDF, pd.DataFrame(
    {
        'id': range(10),
        'a': range(10)
    },
))


def test_insert_rows(dbconn: DBConn):
    mt = MetaTable(
        name='test',
        dbconn=dbconn,
        primary_schema=[
            Column('id', Integer, primary_key=True)
        ]
    )

    new_df, changed_df, new_meta_df, changed_meta_df = mt.get_changes_for_store_chunk(TEST_DF)
    assert_df_equal(new_df, TEST_DF)
    assert(len(changed_df) == 0)

    assert_df_equal(new_meta_df[['id']], new_df[['id']])

    mt.insert_meta_for_store_chunk(new_meta_df=new_meta_df)
    mt.update_meta_for_store_chunk(changed_meta_df=changed_meta_df)

    assert_df_equal(mt.get_metadata()[['id']], TEST_DF[['id']])


def test_sync_meta(dbconn: DBConn):
    mt = MetaTable(
        name='test',
        dbconn=dbconn,
        primary_schema=[
            Column('id', Integer, primary_key=True)
        ]
    )

    new_df, changed_df, new_meta_df, changed_meta_df = mt.get_changes_for_store_chunk(TEST_DF)
    mt.insert_meta_for_store_chunk(new_meta_df=new_meta_df)
    mt.update_meta_for_store_chunk(changed_meta_df=changed_meta_df)

    assert_df_equal(mt.get_metadata()[['id']], TEST_DF[['id']])

    new_df, changed_df, new_meta_df, changed_meta_df = mt.get_changes_for_store_chunk(TEST_DF[:5])
    mt.insert_meta_for_store_chunk(new_meta_df=new_meta_df)
    mt.update_meta_for_store_chunk(changed_meta_df=changed_meta_df)

    deleted_idx = mt.get_changes_for_sync_meta(
        [
            meta_to_index(new_meta_df, ['id']),
            meta_to_index(changed_meta_df, ['id']),
        ]
    )
    assert(len(deleted_idx) == 5)
    mt.update_meta_for_sync_meta(deleted_idx)

    meta_df = mt.get_metadata()
    assert_df_equal(meta_df[['id']], TEST_DF[:5][['id']])
