from typing import cast
import pandas as pd

from sqlalchemy import Integer
from sqlalchemy.sql.schema import Column

from datapipe.metastore import MetaTable
from datapipe.store.database import DBConn
from datapipe.types import DataDF

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

    assert(len(new_df) == len(TEST_DF))
    assert(len(new_meta_df) == len(TEST_DF))
    assert(len(changed_df) == 0)
    assert(len(changed_meta_df) == 0)

    assert_df_equal(new_meta_df[['id']], new_df[['id']])

    mt.insert_meta_for_store_chunk(new_meta_df=new_meta_df)
    mt.update_meta_for_store_chunk(changed_meta_df=changed_meta_df)

    meta_df = mt.get_metadata()

    assert_df_equal(meta_df[['id']], TEST_DF[['id']])

    assert(not meta_df['hash'].isna().any())
