# flake8: noqa

import copy
import time
import pandas as pd
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
    Column('id_left', Integer, primary_key=True),
    Column('a_left', Integer),
]

TEST_SCHEMA_RIGHT = [
    Column('id_right', Integer, primary_key=True),
    Column('b_right', Integer),
]

TEST_SCHEMA_LEFTxRIGHT = [
    Column('id_left', Integer, primary_key=True),
    Column('id_right', Integer, primary_key=True),
    Column('a_left', Integer),
    Column('b_right', Integer),
]

TEST_DF_LEFT = pd.DataFrame(
    {
        'id_left': range(5),
        'a_left': range(5),
    },
)
TEST_DF_LEFT_ADDED = pd.DataFrame(
    {
        'id_left': [6, 7],
        'a_left': [6, 7],
    },
)
TEST_DF_LEFT_FINAL = pd.concat([TEST_DF_LEFT, TEST_DF_LEFT_ADDED], ignore_index=True)

TEST_DF_RIGHT = pd.DataFrame(
    {
        'id_right': [-x for x in range(5)],
        'b_right': [-x for x in range(5)],
    },
)

TEST_DF_RIGHT_ADDED = pd.DataFrame(
    {
        'id_right': [-5, -6, -7, -8],
        'b_right': [-5, -6, -7, -8],
    },
)
TEST_DF_RIGHT_FINAL = pd.concat([TEST_DF_RIGHT, TEST_DF_RIGHT_ADDED], ignore_index=True)


def get_df_cross_merge(df_left, df_right):
    df = pd.merge(df_left, df_right, how='cross')
    df = df.drop_duplicates(subset=['id_left', 'id_right'])
    return df

def test_cross_merge_scenaries(dbconn: DBConn):
    pd.set_option('display.max_columns', None)  # or 1000
    pd.set_option('display.max_rows', None)  # or 1000
    pd.set_option('display.max_colwidth', None)  # or 199
    catalog = Catalog({
        'tbl_left': Table(
            store=TableStoreDB(dbconn, 'id_left', TEST_SCHEMA_LEFT, True)
        ),
        'tbl_right': Table(
            store=TableStoreDB(dbconn, 'tbl_right', TEST_SCHEMA_RIGHT, True)
        ),
        'tbl_left_x_right': Table(
            store=TableStoreDB(dbconn, 'tbl_left_x_right', TEST_SCHEMA_LEFTxRIGHT, True)
        ),
        'tbl_left_x_right_final': Table(
            store=TableStoreDB(dbconn, 'tbl_left_x_right', TEST_SCHEMA_LEFTxRIGHT, True)
        ),
    })
    
    def clean_db():
        for table_name in dbconn.con.table_names():
            dbconn.con.execute(f'DELETE FROM {table_name};').close()

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
        ])
    cross_batch_transform = BatchTransform(
        func=get_df_cross_merge,
        inputs=['tbl_left', 'tbl_right'],
        outputs=['tbl_left_x_right'],
        transform_keys=['id_left', 'id_right']
    )
    ds = DataStore(dbconn, create_meta_table=True)
    cross_step: BatchTransformStep = cross_batch_transform.build_compute(ds, catalog)[0]

    tbl_left = catalog.get_datatable(ds, 'tbl_left')
    tbl_right = catalog.get_datatable(ds, 'tbl_right')
    tbl_left_x_right = catalog.get_datatable(ds, 'tbl_left_x_right')

    # Чистый пайплайн
    run_pipeline(ds, catalog, gen_pipeline(TEST_DF_LEFT, TEST_DF_RIGHT))
    run_steps(ds, [cross_step])
    assert_datatable_equal(tbl_left_x_right, get_df_cross_merge(TEST_DF_LEFT, TEST_DF_RIGHT))

    # Случай 1: меняется что-то слева
    # -> change должно быть равным числу изменненых строк слева помножить на полное число строк справа
    run_pipeline(ds, catalog, gen_pipeline(TEST_DF_LEFT_FINAL, TEST_DF_RIGHT))
    changed_idxs = cross_step.get_changed_idx_count(ds)
    assert changed_idxs == len(TEST_DF_LEFT_ADDED) * len(TEST_DF_RIGHT)
    run_steps(ds, [cross_step])
    assert_datatable_equal(tbl_left_x_right, get_df_cross_merge(TEST_DF_LEFT_FINAL, TEST_DF_RIGHT))

    # # Возвращаем пайплайн к первому состоянию
    clean_db()
    run_pipeline(ds, catalog, gen_pipeline(TEST_DF_LEFT, TEST_DF_RIGHT))
    run_steps(ds, [cross_step])

    # Случай 2: меняется что-то справа
    # -> change должно быть равным полному числу строк слева помножить на измененное число строк справа
    run_pipeline(ds, catalog, gen_pipeline(TEST_DF_LEFT, TEST_DF_RIGHT_FINAL))
    changed_idxs = cross_step.get_changed_idx_count(ds)
    assert changed_idxs == len(TEST_DF_LEFT) * len(TEST_DF_RIGHT_ADDED)
    run_steps(ds, [cross_step])
    assert_datatable_equal(tbl_left_x_right, get_df_cross_merge(TEST_DF_LEFT, TEST_DF_RIGHT_FINAL))

    # Возвращаем пайплайн к первому состоянию
    clean_db()
    run_pipeline(ds, catalog, gen_pipeline(TEST_DF_LEFT, TEST_DF_RIGHT))
    run_steps(ds, [cross_step])

    # Случай 3: меняется что-то и слева, и справа
    # -> change должно быть равным 
    #   - старое полное числу строк слева помножить на измененное число строк справа
    #   плюс
    #   - измененному числу строк помножить на старое полное число строк справа
    #   плюс
    #   - измененное число строк слева помножить на измененное число строк справа
    run_pipeline(ds, catalog, gen_pipeline(TEST_DF_LEFT_FINAL, TEST_DF_RIGHT_FINAL))
    changed_idxs = cross_step.get_changed_idx_count(ds)
    assert changed_idxs == (
        len(TEST_DF_LEFT) * len(TEST_DF_RIGHT_ADDED) +
        len(TEST_DF_RIGHT) * len(TEST_DF_LEFT_ADDED) +
        len(TEST_DF_LEFT_ADDED) * len(TEST_DF_RIGHT_ADDED)
    )
    run_steps(ds, [cross_step])
    assert_datatable_equal(tbl_left_x_right, get_df_cross_merge(TEST_DF_LEFT_FINAL, TEST_DF_RIGHT_FINAL))

    # Случай 4: удаляются какие-то строки и слева, и справа из случая 3
    # -> change должно быть равным 
    #   - старое полное числу строк слева помножить на удаленное число строк справа
    #   плюс
    #   - удаленное числу строк помножить на старое полное число строк справа
    #   плюс
    #   - удаленное число строк слева помножить на удаленное число строк справа
    run_pipeline(ds, catalog, gen_pipeline(TEST_DF_LEFT, TEST_DF_RIGHT))
    changed_idxs = cross_step.get_changed_idx_count(ds)
    assert changed_idxs == (
        len(TEST_DF_LEFT) * len(TEST_DF_RIGHT_ADDED) +
        len(TEST_DF_RIGHT) * len(TEST_DF_LEFT_ADDED) +
        len(TEST_DF_LEFT_ADDED) * len(TEST_DF_RIGHT_ADDED)
    )
    run_steps(ds, [cross_step])
    assert_datatable_equal(tbl_left_x_right, get_df_cross_merge(TEST_DF_LEFT, TEST_DF_RIGHT))
