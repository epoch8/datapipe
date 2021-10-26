from typing import cast, List
import pandas as pd

from sqlalchemy import Integer
from sqlalchemy.sql.schema import Column

from datapipe.metastore import MetaTable
from datapipe.store.database import DBConn
from datapipe.types import DataDF, DataSchema

import pytest
from pytest_cases import parametrize_with_cases, parametrize
from .util import assert_df_equal


class CasesTestDF:
    @parametrize('N', [pytest.param(N) for N in [10, 100, 1000]])
    def case_single_idx(self, N):
        return (
            ['id'],
            [
                Column('id', Integer, primary_key=True),
            ],
            cast(DataDF, pd.DataFrame(
                {
                    'id': range(N),
                    'a': range(N)
                },
            ))
        )

    @parametrize('N', [pytest.param(N) for N in [10, 100, 1000]])
    def case_multi_idx(self, N):
        return (
            ['id1', 'id2'],
            [
                Column('id1', Integer, primary_key=True),
                Column('id2', Integer, primary_key=True)
            ],
            cast(DataDF, pd.DataFrame(
                {
                    'id1': range(N),
                    'id2': range(N),
                    'a': range(N)
                },
            ))
        )


@parametrize_with_cases('index_cols,primary_schema,test_df', cases=CasesTestDF, import_fixtures=True)
def test_insert_rows(dbconn: DBConn, index_cols: List[str], primary_schema: DataSchema, test_df: DataDF):
    mt = MetaTable(
        name='test',
        dbconn=dbconn,
        primary_schema=primary_schema
    )

    new_df, changed_df, new_meta_df, changed_meta_df = mt.get_changes_for_store_chunk(test_df)
    assert_df_equal(new_df, test_df, index_cols=index_cols)
    assert(len(new_df) == len(test_df))
    assert(len(new_meta_df) == len(test_df))
    assert(len(changed_df) == 0)
    assert(len(changed_meta_df) == 0)

    assert_df_equal(new_meta_df[index_cols], new_df[index_cols], index_cols=index_cols)

    mt.insert_meta_for_store_chunk(new_meta_df=new_meta_df)
    mt.update_meta_for_store_chunk(changed_meta_df=changed_meta_df)

    meta_df = mt.get_metadata()

    assert_df_equal(meta_df[index_cols], test_df[index_cols], index_cols=index_cols)

    assert(not meta_df['hash'].isna().any())


@parametrize_with_cases('index_cols,primary_schema,test_df', cases=CasesTestDF, import_fixtures=True)
def test_get_metadata(dbconn: DBConn, index_cols: List[str], primary_schema: DataSchema, test_df: DataDF):
    mt = MetaTable(
        name='test',
        dbconn=dbconn,
        primary_schema=primary_schema
    )

    new_df, changed_df, new_meta_df, changed_meta_df = mt.get_changes_for_store_chunk(test_df)
    mt.insert_meta_for_store_chunk(new_meta_df=new_meta_df)

    part_idx = test_df.iloc[0:2][index_cols]

    assert_df_equal(
        mt.get_metadata(part_idx)[index_cols],
        part_idx,
        index_cols=index_cols
    )
