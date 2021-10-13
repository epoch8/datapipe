from typing import cast
import pandas as pd

from sqlalchemy import Integer
from sqlalchemy.sql.schema import Column

from datapipe.metastore import MetaTable
from datapipe.store.database import DBConn
from datapipe.types import DataDF

import pytest
from pytest_cases import parametrize_with_cases, parametrize
from .util import assert_df_equal


class CasesTestDF:
    @parametrize('N', [pytest.param(N) for N in [10, 100, 1000, 10000]])
    def case_test_df(self, N):
        return cast(DataDF, pd.DataFrame(
            {
                'id': range(N),
                'a': range(N)
            },
        ))


@parametrize_with_cases('test_df', cases=CasesTestDF, import_fixtures=True)
def test_insert_rows(dbconn: DBConn, test_df: DataDF):
    mt = MetaTable(
        name='test',
        dbconn=dbconn,
        primary_schema=[
            Column('id', Integer, primary_key=True)
        ]
    )

    new_df, changed_df, new_meta_df, changed_meta_df = mt.get_changes_for_store_chunk(test_df)
    assert_df_equal(new_df, test_df)
    assert(len(new_df) == len(test_df))
    assert(len(new_meta_df) == len(test_df))
    assert(len(changed_df) == 0)
    assert(len(changed_meta_df) == 0)

    assert_df_equal(new_meta_df[['id']], new_df[['id']])

    mt.insert_meta_for_store_chunk(new_meta_df=new_meta_df)
    mt.update_meta_for_store_chunk(changed_meta_df=changed_meta_df)

    meta_df = mt.get_metadata()

    assert_df_equal(meta_df[['id']], test_df[['id']])

    assert(not meta_df['hash'].isna().any())
