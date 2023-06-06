# flake8: noqa

import copy
import time
from typing import List
import pandas as pd
import pytest
import itertools
from pytest_cases import parametrize, parametrize_with_cases
from datapipe.store.database import DBConn
from datapipe.compute import Catalog, Pipeline, Table, run_pipeline, run_steps
from datapipe.core_steps import BatchGenerate, BatchTransform, BatchTransformStep
from datapipe.datatable import DataStore
from sqlalchemy import Column
from sqlalchemy.sql.sqltypes import Integer

from datapipe.store.database import TableStoreDB
from datapipe.compute import Catalog, Pipeline, Table, run_pipeline, build_compute
from datapipe.core_steps import BatchGenerate, BatchTransform
from .util import assert_datatable_equal


TEST_SCHEMA_LEFT = [
    pytest.param(
        [
            Column('id_left', Integer, primary_key=True),
            Column('a_left', Integer),
        ],
        id="id_left"
    ),
    pytest.param(
        [
            Column('id_left1', Integer, primary_key=True),
            Column('id_left2', Integer, primary_key=True),
            Column('a_left1', Integer),
            Column('a_left2', Integer),
        ],
        id="id_left_x2"
    ),
    pytest.param(
        [
            Column('id_left1', Integer, primary_key=True),
            Column('id_left2', Integer, primary_key=True),
            Column('id_left3', Integer, primary_key=True),
            Column('a_left1', Integer),
            Column('a_left2', Integer),
            Column('a_left3', Integer),
        ],
        id="id_left_x3"
    ),
    pytest.param(
        [
            Column('id_left', Integer, primary_key=True),
            Column('id', Integer, primary_key=True),
            Column('a_left', Integer),
        ],
        id="id_left_with_id"
    ),
    pytest.param(
        [
            Column('id_left', Integer, primary_key=True),
            Column('id1', Integer, primary_key=True),
            Column('id2', Integer, primary_key=True),
            Column('a_left', Integer),
        ],
        id="id_left_with_id_x2"
    )
]

TEST_SCHEMA_RIGHT = [
    pytest.param(
        [
            Column('id_right', Integer, primary_key=True),
            Column('b_right', Integer),
        ],
        id="id_right"
    ),
    pytest.param(
        [
            Column('id_right1', Integer, primary_key=True),
            Column('id_right2', Integer, primary_key=True),
            Column('b_right1', Integer),
            Column('b_right2', Integer),
        ],
        id="id_right_x2"
    ),
    pytest.param(
        [
            Column('id_right1', Integer, primary_key=True),
            Column('id_right2', Integer, primary_key=True),
            Column('id_right3', Integer, primary_key=True),
            Column('b_right1', Integer),
            Column('b_right2', Integer),
            Column('b_right3', Integer),
        ],
        id="id_right_x3"
    ),
    pytest.param(
        [
            Column('id_right', Integer, primary_key=True),
            Column('id', Integer, primary_key=True),
            Column('b_right', Integer),
        ],
        id="id_right_with_id"
    ),
    pytest.param(
        [
            Column('id_right', Integer, primary_key=True),
            Column('id1', Integer, primary_key=True),
            Column('id2', Integer, primary_key=True),
            Column('b_right', Integer),
        ],
        id="id_right_with_id_x2"
    )
]

TEST_DF_LEFT_VALUES = [x for x in range(5)]
TEST_DF_LEFT_ADDED_VALUES = [6, 7]
# TEST_DF_LEFT_FINAL = pd.concat([TEST_DF_LEFT, TEST_DF_LEFT_ADDED], ignore_index=True)

TEST_DF_RIGHT_VALUES = [-x for x in range(5)]
TEST_DF_RIGHT_ADDED_VALUES = [-5, -6, -7, -8]
# TEST_DF_RIGHT_FINAL = pd.concat([TEST_DF_RIGHT, TEST_DF_RIGHT_ADDED], ignore_index=True)


class CasesCrossMerge:
    @parametrize("left_schema", TEST_SCHEMA_LEFT)
    @parametrize("right_schema", TEST_SCHEMA_RIGHT)
    def case_excel(self, dbconn, left_schema: List[Column], right_schema: List[Column]):
        left_columns_primary_keys = [x for x in left_schema if x.primary_key]
        right_columns_primary_keys = [x for x in right_schema if x.primary_key]
        intersecting_idxs = list(
            set([x.name for x in left_columns_primary_keys]).intersection(set([x.name for x in right_columns_primary_keys]))
        )
        left_columns_primary_keys_no_intersecting = [x for x in left_columns_primary_keys if x.name not in intersecting_idxs]
        right_columns_primary_keys_no_intersecting = [x for x in right_columns_primary_keys if x.name not in intersecting_idxs]
        lext_x_right_columns_primary_keys_intersecting = [x for x in left_columns_primary_keys if x.name in intersecting_idxs]
        left_x_right_columns_primary_keys = (
            left_columns_primary_keys_no_intersecting + right_columns_primary_keys_no_intersecting + lext_x_right_columns_primary_keys_intersecting
        )
        left_x_right_columns_nonprimary_keys = [x for x in left_schema if not x.primary_key] + [x for x in right_schema if not x.primary_key]
        left_x_right_schema = left_x_right_columns_primary_keys + left_x_right_columns_nonprimary_keys
        catalog = Catalog({
            'tbl_left': Table(
                store=TableStoreDB(dbconn, 'id_left', left_schema, True)
            ),
            'tbl_right': Table(
                store=TableStoreDB(dbconn, 'tbl_right', right_schema, True)
            ),
            'tbl_left_x_right': Table(
                store=TableStoreDB(dbconn, 'tbl_left_x_right', left_x_right_schema, True)
            ),
        })
        test_df_left = pd.DataFrame({column.name: TEST_DF_LEFT_VALUES for column in left_schema})
        test_df_right = pd.DataFrame({column.name: TEST_DF_RIGHT_VALUES for column in right_schema})
        if len(intersecting_idxs) > 0:
            test_df_left_x_right = pd.merge(test_df_left, test_df_right, on=intersecting_idxs)
        else:
            test_df_left_x_right = pd.merge(test_df_left, test_df_right, how='cross')
        ds = DataStore(dbconn, create_meta_table=True)
        return ds, catalog, test_df_left, test_df_right, test_df_left_x_right, intersecting_idxs


@parametrize_with_cases(
    "ds,catalog,test_df_left,test_df_right,test_df_left_x_right,intersecting_idxs",
    cases=CasesCrossMerge
)
def test_complex_cross_merge_scenaries(
    ds: DataStore,
    catalog: Catalog,
    test_df_left: pd.DataFrame,
    test_df_right: pd.DataFrame,
    test_df_left_x_right: pd.DataFrame,
    intersecting_idxs: List[str]
):  
    def gen_tbl(df):
        yield df

    def clean_db():
        for table_name in ds.meta_dbconn.con.table_names():
            ds.meta_dbconn.con.execute(f'DELETE FROM {table_name};').close()

    tbl_left_x_right = catalog.get_datatable(ds, "tbl_left_x_right")
    if len(intersecting_idxs) > 0:
        primary_keys = intersecting_idxs
    else:
        primary_keys = tbl_left_x_right.table_store.primary_keys
    for len_transform_keys in range(1, len(primary_keys)+1):
        for transform_keys_candidates in itertools.combinations(primary_keys, len_transform_keys):
            transform_keys_case = list(transform_keys_candidates)
            def get_df_cross_merge_case(  # чтобы функция создавалась разная для каждого кейса
                df_left: pd.DataFrame,
                df_right: pd.DataFrame
            ):
                intersection_idxs = list(set(df_left.columns).intersection(set(df_right.columns)))
                if len(intersection_idxs) > 0:
                    df = pd.merge(df_left, df_right, on=intersection_idxs)
                else:
                    df = pd.merge(df_left, df_right, how='cross')
                return df
            get_df_cross_merge_case.__name__ += ("_" + "_".join(transform_keys_case))

            if not (
                any(
                    [key in test_df_left.columns for key in transform_keys_case]
                ) and any(
                    [key in test_df_right.columns for key in transform_keys_case]
                )
            ):
                test_df_left_x_right_case = pd.DataFrame([], columns=test_df_left_x_right.columns)
            else:
                test_df_left_x_right_case = test_df_left_x_right

            pipeline_case = Pipeline([
                BatchGenerate(
                    func=gen_tbl,
                    outputs=['tbl_left'],
                    kwargs=dict(
                        df=test_df_left
                    ),
                ),
                BatchGenerate(
                    func=gen_tbl,
                    outputs=['tbl_right'],
                    kwargs=dict(
                        df=test_df_right
                    ),
                ),
                BatchTransform(
                    func=get_df_cross_merge_case,
                    inputs=['tbl_left', 'tbl_right'],
                    outputs=['tbl_left_x_right'],
                    transform_keys=transform_keys_case,
                    chunk_size=6
                )
            ])
            run_pipeline(ds, catalog, pipeline_case)
            assert_datatable_equal(tbl_left_x_right, test_df_left_x_right_case)
            clean_db()
