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
        intersecting_idxs = set([x.name for x in left_columns_primary_keys]).intersection(set([x.name for x in right_columns_primary_keys]))
        left_columns_primary_keys_no_intersecting = [x for x in left_columns_primary_keys if x.name not in intersecting_idxs]
        right_columns_primary_keys_no_intersecting = [x for x in right_columns_primary_keys if x.name not in intersecting_idxs]
        left_x_right_columns_primary_keys = (
            left_columns_primary_keys_no_intersecting + right_columns_primary_keys_no_intersecting
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
        return ds, catalog, test_df_left, test_df_right, test_df_left_x_right


def get_df_cross_merge(df_left, df_right):
    intersection_idxs = set(df_left.columns).intersection(set(df_right.columns))
    if len(intersection_idxs) > 0:
        df = pd.merge(df_left, df_right, on=intersection_idxs)
    else:
        df = pd.merge(df_left, df_right, how='cross')
    return df


@parametrize_with_cases("ds,catalog,test_df_left,test_df_right,test_df_left_x_right", cases=CasesCrossMerge)
def test_complex_cross_merge_scenaries(ds, catalog, test_df_left, test_df_right, test_df_left_x_right):
    def gen_tbl(df):
        yield df

    def gen_pipeline(df_left, df_right):
        return Pipeline([
            BatchGenerate(
                func=gen_tbl,
                outputs=['tbl_left'],
                kwargs=dict(
                    df=df_left
                ),
            ),
            BatchGenerate(
                func=gen_tbl,
                outputs=['tbl_right'],
                kwargs=dict(
                    df=df_right
                ),
            ),
            BatchTransform(
                func=get_df_cross_merge,
                inputs=['tbl_left', 'tbl_right'],
                outputs=['tbl_left_x_right'],
                transform_keys=[],
                chunk_size=2
            )
        ])
    tbl_left_x_right = catalog.get_datatable(ds, 'tbl_left_x_right')

    # Чистый пайплайн
    run_pipeline(ds, catalog, gen_pipeline(test_df_left, test_df_right))
    assert_datatable_equal(tbl_left_x_right, test_df_left_x_right)

    # # Случай 1: меняется что-то слева
    # # -> change должно быть равным числу изменненых строк слева помножить на полное число строк справа
    # run_pipeline(ds, catalog, gen_pipeline(TEST_DF_LEFT_FINAL, TEST_DF_RIGHT))
    # changed_idxs = cross_step.get_changed_idx_count(ds)
    # assert changed_idxs == len(TEST_DF_LEFT_ADDED) * len(TEST_DF_RIGHT)
    # run_steps(ds, [cross_step])
    # assert_datatable_equal(tbl_left_x_right, get_df_cross_merge(TEST_DF_LEFT_FINAL, TEST_DF_RIGHT))

    # # # Возвращаем пайплайн к первому состоянию
    # clean_db()
    # run_pipeline(ds, catalog, gen_pipeline(TEST_DF_LEFT, TEST_DF_RIGHT))
    # run_steps(ds, [cross_step])

    # # Случай 2: меняется что-то справа
    # # -> change должно быть равным полному числу строк слева помножить на измененное число строк справа
    # run_pipeline(ds, catalog, gen_pipeline(TEST_DF_LEFT, TEST_DF_RIGHT_FINAL))
    # changed_idxs = cross_step.get_changed_idx_count(ds)
    # assert changed_idxs == len(TEST_DF_LEFT) * len(TEST_DF_RIGHT_ADDED)
    # run_steps(ds, [cross_step])
    # assert_datatable_equal(tbl_left_x_right, get_df_cross_merge(TEST_DF_LEFT, TEST_DF_RIGHT_FINAL))

    # # Возвращаем пайплайн к первому состоянию
    # clean_db()
    # run_pipeline(ds, catalog, gen_pipeline(TEST_DF_LEFT, TEST_DF_RIGHT))
    # run_steps(ds, [cross_step])

    # # Случай 3: меняется что-то и слева, и справа
    # # -> change должно быть равным 
    # #   - старое полное числу строк слева помножить на измененное число строк справа
    # #   плюс
    # #   - измененному числу строк помножить на старое полное число строк справа
    # #   плюс
    # #   - измененное число строк слева помножить на измененное число строк справа
    # run_pipeline(ds, catalog, gen_pipeline(TEST_DF_LEFT_FINAL, TEST_DF_RIGHT_FINAL))
    # changed_idxs = cross_step.get_changed_idx_count(ds)
    # assert changed_idxs == (
    #     len(TEST_DF_LEFT) * len(TEST_DF_RIGHT_ADDED) +
    #     len(TEST_DF_RIGHT) * len(TEST_DF_LEFT_ADDED) +
    #     len(TEST_DF_LEFT_ADDED) * len(TEST_DF_RIGHT_ADDED)
    # )
    # run_steps(ds, [cross_step])
    # assert_datatable_equal(tbl_left_x_right, get_df_cross_merge(TEST_DF_LEFT_FINAL, TEST_DF_RIGHT_FINAL))

    # # Случай 4: удаляются какие-то строки и слева, и справа из случая 3
    # # -> change должно быть равным 
    # #   - старое полное числу строк слева помножить на удаленное число строк справа
    # #   плюс
    # #   - удаленное числу строк помножить на старое полное число строк справа
    # #   плюс
    # #   - удаленное число строк слева помножить на удаленное число строк справа
    # run_pipeline(ds, catalog, gen_pipeline(TEST_DF_LEFT, TEST_DF_RIGHT))
    # changed_idxs = cross_step.get_changed_idx_count(ds)
    # assert changed_idxs == (
    #     len(TEST_DF_LEFT) * len(TEST_DF_RIGHT_ADDED) +
    #     len(TEST_DF_RIGHT) * len(TEST_DF_LEFT_ADDED) +
    #     len(TEST_DF_LEFT_ADDED) * len(TEST_DF_RIGHT_ADDED)
    # )
    # run_steps(ds, [cross_step])
    # assert_datatable_equal(tbl_left_x_right, get_df_cross_merge(TEST_DF_LEFT, TEST_DF_RIGHT))
