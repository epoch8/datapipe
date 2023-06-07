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
from .util import assert_datatable_equal, assert_df_equal


TEST_SCHEMA_LEFT = [
    pytest.param(
        [
            Column('id_left', Integer, primary_key=True),
            Column('a_left', Integer),
        ],
        id="left"
    ),
    pytest.param(
        [
            Column('id_left1', Integer, primary_key=True),
            Column('id_left2', Integer, primary_key=True),
            Column('a_left1', Integer),
            Column('a_left2', Integer),
        ],
        id="left_x2"
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
        id="left_x3"
    ),
    pytest.param(
        [
            Column('id_left', Integer, primary_key=True),
            Column('id', Integer, primary_key=True),
            Column('a_left', Integer),
        ],
        id="left_with_id"
    ),
    pytest.param(
        [
            Column('id_left', Integer, primary_key=True),
            Column('id1', Integer, primary_key=True),
            Column('id2', Integer, primary_key=True),
            Column('a_left', Integer),
        ],
        id="left_with_id_x2"
    )
]

TEST_SCHEMA_RIGHT = [
    pytest.param(
        [
            Column('id_right', Integer, primary_key=True),
            Column('b_right', Integer),
        ],
        id="right"
    ),
    pytest.param(
        [
            Column('id_right1', Integer, primary_key=True),
            Column('id_right2', Integer, primary_key=True),
            Column('b_right1', Integer),
            Column('b_right2', Integer),
        ],
        id="right_x2"
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
        id="right_x3"
    ),
    pytest.param(
        [
            Column('id_right', Integer, primary_key=True),
            Column('id', Integer, primary_key=True),
            Column('b_right', Integer),
        ],
        id="right_with_id"
    ),
    pytest.param(
        [
            Column('id_right', Integer, primary_key=True),
            Column('id1', Integer, primary_key=True),
            Column('id2', Integer, primary_key=True),
            Column('b_right', Integer),
        ],
        id="right_with_id_x2"
    )
]

TEST_DF_LEFT_VALUES = [x for x in range(5)] + [6, 7, -5, -9]
TEST_DF_RIGHT_VALUES = [-x for x in range(5)] + [5, -6, 7, -8, 10, 12]


def get_all_cases():
    for left_schema_param in TEST_SCHEMA_LEFT:
        for right_schema_param in TEST_SCHEMA_RIGHT:
            left_schema = left_schema_param.values[0]
            right_schema = right_schema_param.values[0]
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
            test_df_left = pd.DataFrame({column.name: TEST_DF_LEFT_VALUES for column in left_schema})
            test_df_right = pd.DataFrame({column.name: TEST_DF_RIGHT_VALUES for column in right_schema})
            if len(intersecting_idxs) > 0:
                test_df_left_x_right = pd.merge(test_df_left, test_df_right, on=intersecting_idxs)
                primary_keys = intersecting_idxs
            else:
                test_df_left_x_right = pd.merge(test_df_left, test_df_right, how='cross')
                primary_keys = [x.name for x in left_x_right_columns_primary_keys]
            for len_transform_keys in range(1, len(primary_keys)+1):
                for transform_keys in itertools.combinations(primary_keys, len_transform_keys):
                    id_transform_keys = "_".join(transform_keys)
                        
                    yield pytest.param(
                        [
                            left_schema, right_schema, left_x_right_schema,
                            test_df_left, test_df_right, test_df_left_x_right,
                            transform_keys
                        ],
                        id=f"{left_schema_param.id}-{right_schema_param.id}-trasnforms-keys-{id_transform_keys}"
                    )


def cross_merge_func(df_left: pd.DataFrame, df_right: pd.DataFrame):
    intersection_idxs = list(set(df_left.columns).intersection(set(df_right.columns)))
    if len(intersection_idxs) > 0:
        df = pd.merge(df_left, df_right, on=intersection_idxs)
    else:
        df = pd.merge(df_left, df_right, how='cross')
    return df


@parametrize("test_case",list(get_all_cases()))
def test_complex_cross_merge_scenary(dbconn, test_case):
    (
        left_schema, right_schema, left_x_right_schema,
        test_df_left, test_df_right, test_df_left_x_right,
        transform_keys
    ) = test_case
    ds = DataStore(dbconn, create_meta_table=True)
    catalog = Catalog({
        'tbl_left': Table(
            store=TableStoreDB(dbconn, 'tbl_left', left_schema, True)
        ),
        'tbl_right': Table(
            store=TableStoreDB(dbconn, 'tbl_right', right_schema, True)
        ),
        'tbl_left_x_right': Table(
            store=TableStoreDB(dbconn, 'tbl_left_x_right', left_x_right_schema, True)
        ),
    })

    def gen_tbl(df):
        yield df

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
            func=cross_merge_func,
            inputs=['tbl_left', 'tbl_right'],
            outputs=['tbl_left_x_right'],
            transform_keys=transform_keys,
            chunk_size=6
        )
    ])
    run_pipeline(ds, catalog, pipeline_case)
    tbl_left = catalog.get_datatable(ds, "tbl_left")
    tbl_right = catalog.get_datatable(ds, "tbl_right")
    tbl_left_x_right = catalog.get_datatable(ds, "tbl_left_x_right")
    assert_datatable_equal(tbl_left, test_df_left)
    assert_datatable_equal(tbl_right, test_df_right)
    assert_datatable_equal(tbl_left_x_right, test_df_left_x_right)


def reverse_cross_merge_func(
    df_left_x_right: pd.DataFrame,
    left_schema: List[Column],
    right_schema: List[Column]
):
    df_left = df_left_x_right[[x.name for x in left_schema]].drop_duplicates()
    df_right = df_left_x_right[[x.name for x in right_schema]].drop_duplicates()
    return df_left, df_right


@parametrize("test_case",list(get_all_cases()))
def test_complex_reverse_cross_merge_scenary(dbconn, test_case):
    (
        left_schema, right_schema, left_x_right_schema,
        test_df_left, test_df_right, test_df_left_x_right,
        transform_keys
    ) = test_case
    ds = DataStore(dbconn, create_meta_table=True)
    catalog = Catalog({
        'tbl_left_x_right': Table(
            store=TableStoreDB(dbconn, 'tbl_left_x_right', left_x_right_schema, True)
        ),
        'tbl_left': Table(
            store=TableStoreDB(dbconn, 'tbl_left', left_schema, True)
        ),
        'tbl_right': Table(
            store=TableStoreDB(dbconn, 'tbl_right', right_schema, True)
        ),
    })

    def gen_tbl(df):
        yield df

    pipeline_case = Pipeline([
        BatchGenerate(
            func=gen_tbl,
            outputs=['tbl_left_x_right'],
            kwargs=dict(
                df=test_df_left_x_right
            ),
        ),
        BatchTransform(
            func=reverse_cross_merge_func,
            inputs=['tbl_left_x_right'],
            outputs=['tbl_left', 'tbl_right'],
            transform_keys=transform_keys,
            chunk_size=6,
            kwargs=dict(
                left_schema=left_schema,
                right_schema=right_schema
            )
        )
    ])
    # run_pipeline(ds, catalog, pipeline_case)
    tbl_left_x_right = catalog.get_datatable(ds, "tbl_left_x_right")
    tbl_left = catalog.get_datatable(ds, "tbl_left")
    tbl_right = catalog.get_datatable(ds, "tbl_right")
    df_left, df_right = reverse_cross_merge_func(test_df_left_x_right, left_schema, right_schema)
    assert_df_equal(df_left, test_df_left, index_cols=[x.name for x in left_schema if x.primary_key])
    assert_df_equal(df_right, test_df_right, index_cols=[x.name for x in right_schema if x.primary_key])
    assert_datatable_equal(tbl_left_x_right, test_df_left_x_right)
    assert_datatable_equal(tbl_left, test_df_left)
    assert_datatable_equal(tbl_right, test_df_right)
