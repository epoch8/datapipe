from typing import Iterator, Tuple

import pandas as pd
import pytest
from sqlalchemy import Column
from sqlalchemy.sql.sqltypes import Integer

from datapipe.compute import (
    Catalog,
    Pipeline,
    Table,
    run_pipeline,
    run_steps,
    run_steps_changelist,
)
from datapipe.datatable import DataStore, DataTable
from datapipe.step.batch_generate import BatchGenerate
from datapipe.step.batch_transform import BatchTransform, BatchTransformStep
from datapipe.store.database import DBConn, TableStoreDB
from datapipe.tests.util import assert_datatable_equal
from datapipe.types import ChangeList

TEST_SCHEMA_LEFT = [
    Column("id_left", Integer, primary_key=True),
    Column("a_left", Integer),
]

TEST_SCHEMA_RIGHT = [
    Column("id_right", Integer, primary_key=True),
    Column("b_right", Integer),
]

TEST_SCHEMA_LEFTxRIGHT = [
    Column("id_left", Integer, primary_key=True),
    Column("id_right", Integer, primary_key=True),
    Column("a_left", Integer),
    Column("b_right", Integer),
]

TEST_DF_LEFT = pd.DataFrame(
    {
        "id_left": range(5),
        "a_left": range(5),
    },
)
TEST_DF_LEFT_ADDED = pd.DataFrame(
    {
        "id_left": [6, 7],
        "a_left": [6, 7],
    },
)
TEST_DF_LEFT_FINAL = pd.concat([TEST_DF_LEFT, TEST_DF_LEFT_ADDED], ignore_index=True)

TEST_DF_RIGHT = pd.DataFrame(
    {
        "id_right": [-x for x in range(5)],
        "b_right": [-x for x in range(5)],
    },
)

TEST_DF_RIGHT_ADDED = pd.DataFrame(
    {
        "id_right": [-5, -6, -7, -8],
        "b_right": [-5, -6, -7, -8],
    },
)
TEST_DF_RIGHT_FINAL = pd.concat([TEST_DF_RIGHT, TEST_DF_RIGHT_ADDED], ignore_index=True)


def get_df_cross_merge(df_left, df_right):
    df = pd.merge(df_left, df_right, how="cross")
    df = df.drop_duplicates(subset=["id_left", "id_right"])
    return df


def gen_tbl(df):
    yield df


def gen_pipeline(df_left, df_right):
    return Pipeline(
        [
            BatchGenerate(
                func=gen_tbl,
                outputs=["tbl_left"],
                kwargs=dict(df=df_left),
            ),
            BatchGenerate(
                func=gen_tbl,
                outputs=["tbl_right"],
                kwargs=dict(df=df_right),
            ),
        ]
    )


@pytest.fixture
def ds_catalog_pipeline_tbls(
    dbconn: DBConn,
) -> Iterator[Tuple[DataStore, Catalog, DataTable, DataTable, DataTable, BatchTransformStep]]:
    catalog = Catalog(
        {
            "tbl_left": Table(store=TableStoreDB(dbconn, "id_left", TEST_SCHEMA_LEFT, True)),
            "tbl_right": Table(store=TableStoreDB(dbconn, "tbl_right", TEST_SCHEMA_RIGHT, True)),
            "tbl_left_x_right": Table(store=TableStoreDB(dbconn, "tbl_left_x_right", TEST_SCHEMA_LEFTxRIGHT, True)),
            "tbl_left_x_right_final": Table(
                store=TableStoreDB(dbconn, "tbl_left_x_right", TEST_SCHEMA_LEFTxRIGHT, True)
            ),
        }
    )

    cross_batch_transform = BatchTransform(
        func=get_df_cross_merge,
        inputs=["tbl_left", "tbl_right"],
        outputs=["tbl_left_x_right"],
        transform_keys=["id_left", "id_right"],
    )
    ds = DataStore(dbconn, create_meta_table=True)
    cross_step = cross_batch_transform.build_compute(ds, catalog)[0]
    assert isinstance(cross_step, BatchTransformStep)

    tbl_left = catalog.get_datatable(ds, "tbl_left")
    tbl_right = catalog.get_datatable(ds, "tbl_right")
    tbl_left_x_right = catalog.get_datatable(ds, "tbl_left_x_right")
    yield ds, catalog, tbl_left, tbl_right, tbl_left_x_right, cross_step


def test_cross_merge_scenary_clear(ds_catalog_pipeline_tbls):
    (
        ds,
        catalog,
        tbl_left,
        tbl_right,
        tbl_left_x_right,
        cross_step,
    ) = ds_catalog_pipeline_tbls
    # Чистый пайплайн
    run_pipeline(ds, catalog, gen_pipeline(TEST_DF_LEFT, TEST_DF_RIGHT))
    run_steps(ds, [cross_step])
    assert_datatable_equal(tbl_left_x_right, get_df_cross_merge(TEST_DF_LEFT, TEST_DF_RIGHT))


def test_cross_merge_scenary_clear_changelist(ds_catalog_pipeline_tbls):
    (
        ds,
        catalog,
        tbl_left,
        tbl_right,
        tbl_left_x_right,
        cross_step,
    ) = ds_catalog_pipeline_tbls
    # Чистый пайплайн
    run_pipeline(ds, catalog, gen_pipeline(TEST_DF_LEFT, TEST_DF_RIGHT))
    changelist = ChangeList()
    changelist.append("tbl_left", idx=tbl_left.get_data())
    run_steps_changelist(ds, [cross_step], changelist)
    assert_datatable_equal(tbl_left_x_right, get_df_cross_merge(TEST_DF_LEFT, TEST_DF_RIGHT))


def test_cross_merge_scenary_clear_changelist_null_values_check(
    ds_catalog_pipeline_tbls,
):
    (
        ds,
        catalog,
        tbl_left,
        tbl_right,
        tbl_left_x_right,
        cross_step,
    ) = ds_catalog_pipeline_tbls
    # Добавляем 1ую табличку, вторая таблица пустая
    df_idx_left = tbl_left.store_chunk(TEST_DF_LEFT)
    changelist = ChangeList()
    changelist.append("tbl_left", df_idx_left)
    run_steps_changelist(ds, [cross_step], changelist)
    changelist = ChangeList()
    changelist.append("tbl_left", TEST_DF_LEFT_ADDED)  # притворяемся, что "данные есть"
    run_steps_changelist(ds, [cross_step], changelist)

    # Добавляем 2ую табличку
    df_idx_right = tbl_right.store_chunk(TEST_DF_RIGHT)
    changelist = ChangeList()
    changelist.append("tbl_right", df_idx_right)
    run_steps_changelist(ds, [cross_step], changelist)
    assert_datatable_equal(tbl_left_x_right, get_df_cross_merge(TEST_DF_LEFT, TEST_DF_RIGHT))
    changelist = ChangeList()
    changelist.append("tbl_right", TEST_DF_RIGHT_ADDED)  # притворяемся, что "данные есть"
    run_steps_changelist(ds, [cross_step], changelist)

    changelist = ChangeList()
    changelist.append("tbl_left", TEST_DF_LEFT_ADDED)  # притворяемся, что "данные есть"
    changelist.append("tbl_right", TEST_DF_RIGHT_ADDED)  # притворяемся, что "данные есть"
    run_steps_changelist(ds, [cross_step], changelist)

    changelist = ChangeList()
    changelist.append("tbl_left", TEST_DF_LEFT_FINAL)  # смесь реальных и пустых индексов
    changelist.append("tbl_right", TEST_DF_RIGHT_FINAL)  # смесь реальных и пустых индексов
    run_steps_changelist(ds, [cross_step], changelist)


def test_cross_merge_scenary_changed_left(ds_catalog_pipeline_tbls):
    (
        ds,
        catalog,
        tbl_left,
        tbl_right,
        tbl_left_x_right,
        cross_step,
    ) = ds_catalog_pipeline_tbls
    test_cross_merge_scenary_clear(ds_catalog_pipeline_tbls)
    # Случай 1: меняется что-то слева
    # -> change должно быть равным числу изменненых строк слева помножить на полное число строк справа
    run_pipeline(ds, catalog, gen_pipeline(TEST_DF_LEFT_FINAL, TEST_DF_RIGHT))
    changed_idxs = cross_step.meta.get_changed_idx_count(ds)
    assert changed_idxs == len(TEST_DF_LEFT_ADDED) * len(TEST_DF_RIGHT)
    run_steps(ds, [cross_step])
    assert_datatable_equal(tbl_left_x_right, get_df_cross_merge(TEST_DF_LEFT_FINAL, TEST_DF_RIGHT))


def test_cross_merge_scenary_changed_right(ds_catalog_pipeline_tbls):
    (
        ds,
        catalog,
        tbl_left,
        tbl_right,
        tbl_left_x_right,
        cross_step,
    ) = ds_catalog_pipeline_tbls
    test_cross_merge_scenary_clear(ds_catalog_pipeline_tbls)
    # Случай 2: меняется что-то справа
    # -> change должно быть равным полному числу строк слева помножить на измененное число строк справа
    run_pipeline(ds, catalog, gen_pipeline(TEST_DF_LEFT, TEST_DF_RIGHT_FINAL))
    changed_idxs = cross_step.meta.get_changed_idx_count(ds)
    assert changed_idxs == len(TEST_DF_LEFT) * len(TEST_DF_RIGHT_ADDED)
    run_steps(ds, [cross_step])
    assert_datatable_equal(tbl_left_x_right, get_df_cross_merge(TEST_DF_LEFT, TEST_DF_RIGHT_FINAL))


def test_cross_merge_scenary_changed_left_and_right(ds_catalog_pipeline_tbls):
    (
        ds,
        catalog,
        tbl_left,
        tbl_right,
        tbl_left_x_right,
        cross_step,
    ) = ds_catalog_pipeline_tbls
    test_cross_merge_scenary_clear(ds_catalog_pipeline_tbls)
    # Случай 3: меняется что-то и слева, и справа
    # -> change должно быть равным
    #   - старое полное числу строк слева помножить на измененное число строк справа
    #   плюс
    #   - измененному числу строк помножить на старое полное число строк справа
    #   плюс
    #   - измененное число строк слева помножить на измененное число строк справа
    run_pipeline(ds, catalog, gen_pipeline(TEST_DF_LEFT_FINAL, TEST_DF_RIGHT_FINAL))
    changed_idxs = cross_step.meta.get_changed_idx_count(ds)
    assert changed_idxs == (
        len(TEST_DF_LEFT) * len(TEST_DF_RIGHT_ADDED)
        + len(TEST_DF_RIGHT) * len(TEST_DF_LEFT_ADDED)
        + len(TEST_DF_LEFT_ADDED) * len(TEST_DF_RIGHT_ADDED)
    )
    run_steps(ds, [cross_step])
    assert_datatable_equal(tbl_left_x_right, get_df_cross_merge(TEST_DF_LEFT_FINAL, TEST_DF_RIGHT_FINAL))


def test_cross_merge_scenary_changed_left_and_right_then_deleted_left_and_right(
    ds_catalog_pipeline_tbls,
):
    (
        ds,
        catalog,
        tbl_left,
        tbl_right,
        tbl_left_x_right,
        cross_step,
    ) = ds_catalog_pipeline_tbls
    test_cross_merge_scenary_changed_left_and_right(ds_catalog_pipeline_tbls)
    # Случай 4: удаляются какие-то строки и слева, и справа из случая 3
    # -> change должно быть равным
    #   - старое полное числу строк слева помножить на удаленное число строк справа
    #   плюс
    #   - удаленное числу строк помножить на старое полное число строк справа
    #   плюс
    #   - удаленное число строк слева помножить на удаленное число строк справа
    run_pipeline(ds, catalog, gen_pipeline(TEST_DF_LEFT, TEST_DF_RIGHT))
    changed_idxs = cross_step.meta.get_changed_idx_count(ds)
    assert changed_idxs == (
        len(TEST_DF_LEFT) * len(TEST_DF_RIGHT_ADDED)
        + len(TEST_DF_RIGHT) * len(TEST_DF_LEFT_ADDED)
        + len(TEST_DF_LEFT_ADDED) * len(TEST_DF_RIGHT_ADDED)
    )
    run_steps(ds, [cross_step])
    assert_datatable_equal(tbl_left_x_right, get_df_cross_merge(TEST_DF_LEFT, TEST_DF_RIGHT))
