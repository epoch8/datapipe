from typing import List, cast

import pandas as pd
import pytest
from pytest_cases import parametrize, parametrize_with_cases
from sqlalchemy import Integer
from sqlalchemy.sql.schema import Column

from datapipe.meta.sql_meta import SQLTableMeta
from datapipe.store.database import DBConn, MetaKey
from datapipe.tests.util import assert_df_equal
from datapipe.types import DataSchema, HashDF, IndexDF, MetaSchema, hash_to_index


class CasesTestDF:
    @parametrize("N", [pytest.param(N) for N in [10, 100, 1000]])
    def case_single_idx(self, N):
        return (
            ["id"],
            [
                Column("id", Integer, primary_key=True),
            ],
            [],
            cast(
                HashDF,
                pd.DataFrame(
                    {"id": range(N), "hash": range(N)},
                ),
            ),
        )

    @parametrize("N", [pytest.param(N) for N in [10, 100, 1000]])
    def case_single_idx_with_meta(self, N):
        return (
            ["id"],
            [
                Column("id", Integer, primary_key=True),
            ],
            [
                Column("item_id", Integer, MetaKey()),
            ],
            cast(
                HashDF,
                pd.DataFrame(
                    {"id": range(N), "item_id": range(N), "hash": range(N)},
                ),
            ),
        )

    @parametrize("N", [pytest.param(N) for N in [10, 100, 1000]])
    def case_multi_idx(self, N):
        return (
            ["id1", "id2"],
            [
                Column("id1", Integer, primary_key=True),
                Column("id2", Integer, primary_key=True),
            ],
            [],
            cast(
                HashDF,
                pd.DataFrame(
                    {"id1": range(N), "id2": range(N), "hash": range(N)},
                ),
            ),
        )

    @parametrize("N", [pytest.param(N) for N in [10, 100, 1000]])
    def case_multi_idx_with_meta(self, N):
        return (
            ["id1", "id2"],
            [
                Column("id1", Integer, primary_key=True),
                Column("id2", Integer, primary_key=True),
            ],
            [
                Column("item_id", Integer, MetaKey()),
                Column("product_id", Integer, MetaKey()),
            ],
            cast(
                HashDF,
                pd.DataFrame(
                    {
                        "id1": range(N),
                        "id2": range(N),
                        "item_id": range(N),
                        "product_id": range(N),
                        "hash": range(N),
                    },
                ),
            ),
        )


@parametrize_with_cases(
    "index_cols,primary_schema,meta_schema,test_df",
    cases=CasesTestDF,
    import_fixtures=True,
)
def test_insert_rows(
    dbconn: DBConn,
    index_cols: List[str],
    primary_schema: DataSchema,
    meta_schema: MetaSchema,
    test_df: HashDF,
):
    mt = SQLTableMeta(
        name="test",
        dbconn=dbconn,
        primary_schema=primary_schema,
        meta_schema=meta_schema,
        create_table=True,
    )
    keys = list(set(mt.primary_keys) | set(mt.meta_keys.keys()))
    new_index_df, changed_index_df, new_meta_df, changed_meta_df = mt.get_changes_for_store_chunk(test_df)

    assert_df_equal(new_index_df, hash_to_index(test_df, index_cols), index_cols=index_cols)
    assert len(new_index_df) == len(test_df)
    assert len(new_meta_df) == len(test_df)
    assert len(changed_index_df) == 0
    assert len(changed_meta_df) == 0

    assert_df_equal(new_meta_df[index_cols], new_index_df, index_cols=index_cols)
    assert_df_equal(new_meta_df[keys], test_df[keys], index_cols=index_cols)

    mt.update_rows(df=new_meta_df)
    mt.update_rows(df=changed_meta_df)

    meta_df = mt.get_metadata()

    assert_df_equal(meta_df[index_cols], test_df[index_cols], index_cols=index_cols)
    assert_df_equal(meta_df[keys], test_df[keys], index_cols=index_cols)

    assert not meta_df["hash"].isna().any()


@parametrize_with_cases(
    "index_cols,primary_schema,meta_schema,test_df",
    cases=CasesTestDF,
    import_fixtures=True,
)
def test_get_metadata(
    dbconn: DBConn,
    index_cols: List[str],
    primary_schema: DataSchema,
    meta_schema: MetaSchema,
    test_df: HashDF,
):
    mt = SQLTableMeta(
        name="test",
        dbconn=dbconn,
        primary_schema=primary_schema,
        meta_schema=meta_schema,
        create_table=True,
    )

    new_df, changed_df, new_meta_df, changed_meta_df = mt.get_changes_for_store_chunk(test_df)
    mt.update_rows(df=new_meta_df)

    part_df = test_df.iloc[0:2]
    part_idx = part_df[index_cols]
    keys = list(set(mt.primary_keys) | set(mt.meta_keys.keys()))

    assert_df_equal(
        mt.get_metadata(cast(IndexDF, part_idx))[index_cols],
        part_idx,
        index_cols=index_cols,
    )

    assert_df_equal(
        mt.get_metadata(cast(IndexDF, part_idx))[keys],
        part_df[keys],
        index_cols=index_cols,
    )
