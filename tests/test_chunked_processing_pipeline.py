import os
from typing import List, Optional

import numpy as np
import pandas as pd
import pytest
from sqlalchemy import Column
from sqlalchemy.sql.sqltypes import Integer

from datapipe.compute import (
    Catalog,
    Pipeline,
    Table,
    build_compute,
    run_changelist,
    run_steps,
    run_steps_changelist,
)
from datapipe.datatable import DataStore, DataTable
from datapipe.run_config import RunConfig
from datapipe.step.batch_generate import BatchGenerate
from datapipe.step.batch_transform import BatchTransform
from datapipe.step.datatable_transform import DatatableTransform
from datapipe.step.update_external_table import UpdateExternalTable
from datapipe.store.database import TableStoreDB
from datapipe.store.pandas import TableStoreJsonLine
from datapipe.types import ChangeList, data_to_index

from .util import assert_datatable_equal, assert_df_equal

CHUNK_SIZE = 100
CHUNK_SIZE_SMALL = 3

TEST_SCHEMA = [
    Column("id", Integer, primary_key=True),
    Column("a", Integer),
]

TEST_DF = pd.DataFrame(
    {
        "id": range(10),
        "a": range(10),
    },
)


def test_table_store_json_line_reading(tmp_dir, dbconn):
    def conversion(df, multiply):
        df["y"] = df["x"] ** multiply
        return df

    x = pd.Series(np.arange(2 * CHUNK_SIZE, dtype=np.int32))
    test_df = pd.DataFrame(
        {
            "id": x.apply(str),
            "x": x,
        }
    )
    test_input_fname = os.path.join(tmp_dir, "table-input-pandas.json")
    test_output_fname = os.path.join(tmp_dir, "table-output-pandas.json")
    test_df.to_json(test_input_fname, orient="records", lines=True)

    ds = DataStore(dbconn, create_meta_table=True)
    catalog = Catalog(
        {
            "input_data": Table(
                store=TableStoreJsonLine(test_input_fname),
            ),
            "output_data": Table(
                store=TableStoreJsonLine(test_output_fname),
            ),
        }
    )
    pipeline = Pipeline(
        [
            UpdateExternalTable("input_data"),
            BatchTransform(
                conversion,
                inputs=["input_data"],
                outputs=["output_data"],
                chunk_size=CHUNK_SIZE,
                kwargs=dict(
                    multiply=2,
                ),
            ),
        ]
    )

    steps = build_compute(ds, catalog, pipeline)
    run_steps(ds, steps)

    df_transformed = catalog.get_datatable(ds, "output_data").get_data()
    assert len(df_transformed) == 2 * CHUNK_SIZE
    assert all(df_transformed["y"].values == (df_transformed["x"].values ** 2))
    assert len(set(df_transformed["x"].values).symmetric_difference(set(x))) == 0


def test_transform_with_many_input_and_output_tables(tmp_dir, dbconn):
    ds = DataStore(dbconn, create_meta_table=True)
    catalog = Catalog(
        {
            "inp1": Table(
                store=TableStoreDB(
                    dbconn,
                    "inp1_data",
                    TEST_SCHEMA,
                    create_table=True,
                )
            ),
            "inp2": Table(
                store=TableStoreDB(
                    dbconn,
                    "inp2_data",
                    TEST_SCHEMA,
                    create_table=True,
                )
            ),
            "out1": Table(
                store=TableStoreDB(
                    dbconn,
                    "out1_data",
                    TEST_SCHEMA,
                    create_table=True,
                )
            ),
            "out2": Table(
                store=TableStoreDB(
                    dbconn,
                    "out2_data",
                    TEST_SCHEMA,
                    create_table=True,
                )
            ),
        }
    )

    def transform(df1, df2):
        return df1, df2

    pipeline = Pipeline(
        [
            BatchTransform(
                transform,
                inputs=["inp1", "inp2"],
                outputs=["out1", "out2"],
                chunk_size=CHUNK_SIZE,
            ),
        ]
    )

    catalog.get_datatable(ds, "inp1").store_chunk(TEST_DF)
    catalog.get_datatable(ds, "inp2").store_chunk(TEST_DF)

    steps = build_compute(ds, catalog, pipeline)
    run_steps(ds, steps)

    out1 = catalog.get_datatable(ds, "out1")
    out2 = catalog.get_datatable(ds, "out2")

    assert_datatable_equal(out1, TEST_DF)
    assert_datatable_equal(out2, TEST_DF)


def test_run_changelist_simple(dbconn):
    ds = DataStore(dbconn, create_meta_table=True)
    catalog = Catalog(
        {
            "inp": Table(
                store=TableStoreDB(
                    dbconn,
                    "inp_data",
                    TEST_SCHEMA,
                    create_table=True,
                )
            ),
            "out": Table(
                store=TableStoreDB(
                    dbconn,
                    "out_data",
                    TEST_SCHEMA,
                    create_table=True,
                )
            ),
        }
    )

    def transform(df):
        return df

    pipeline = Pipeline(
        [
            BatchTransform(
                transform,
                inputs=["inp"],
                outputs=["out"],
                chunk_size=CHUNK_SIZE,
            ),
        ]
    )

    changeIdx = data_to_index(TEST_DF.loc[[2, 3, 4]], ["id"])
    changelist = ChangeList.create("inp", changeIdx)

    catalog.get_datatable(ds, "inp").store_chunk(TEST_DF, now=0)

    run_changelist(ds, catalog, pipeline, changelist)

    assert_datatable_equal(
        catalog.get_datatable(ds, "out"), TEST_DF.loc[changeIdx.index]
    )


def test_run_changelist_with_duplicate_input_keys(dbconn):
    ds = DataStore(dbconn, create_meta_table=True)
    catalog = Catalog(
        {
            "inp": Table(
                store=TableStoreDB(
                    dbconn,
                    "inp_data",
                    TEST_SCHEMA,
                    create_table=True,
                )
            ),
        }
    )

    test_df_with_duplicates = pd.DataFrame(
        {
            "id": [1, 1],
            "a": [1, 2],
        }
    )

    dt = catalog.get_datatable(ds, "inp")

    dt.store_chunk(TEST_DF)

    with pytest.raises(ValueError):
        dt.store_chunk(test_df_with_duplicates)

    assert_datatable_equal(dt, TEST_DF)


def test_run_changelist_by_chunk_size_simple(dbconn):
    ds = DataStore(dbconn, create_meta_table=True)
    catalog = Catalog(
        {
            "inp": Table(
                store=TableStoreDB(
                    dbconn,
                    "inp_data",
                    TEST_SCHEMA,
                    create_table=True,
                )
            ),
            "out": Table(
                store=TableStoreDB(
                    dbconn,
                    "out_data",
                    TEST_SCHEMA,
                    create_table=True,
                )
            ),
        }
    )

    def transform(df):
        if len(df) > CHUNK_SIZE_SMALL:
            raise Exception("Test chunk size")

        return df

    pipeline = Pipeline(
        [
            BatchTransform(
                transform,
                inputs=["inp"],
                outputs=["out"],
                chunk_size=CHUNK_SIZE_SMALL,
            ),
        ]
    )

    changeIdx = data_to_index(TEST_DF.loc[[2, 3, 4, 5, 6]], ["id"])
    changelist = ChangeList.create("inp", changeIdx)

    catalog.get_datatable(ds, "inp").store_chunk(TEST_DF, now=0)

    run_changelist(ds, catalog, pipeline, changelist)

    assert_datatable_equal(
        catalog.get_datatable(ds, "out"), TEST_DF.loc[changeIdx.index]
    )


def test_run_changelist_cycle(dbconn):
    ds = DataStore(dbconn, create_meta_table=True)
    catalog = Catalog(
        {
            "a": Table(
                store=TableStoreDB(
                    dbconn,
                    "a_data",
                    TEST_SCHEMA,
                    create_table=True,
                )
            ),
            "b": Table(
                store=TableStoreDB(
                    dbconn,
                    "b_data",
                    TEST_SCHEMA,
                    create_table=True,
                )
            ),
        }
    )

    def inc(df):
        return df.assign(a=df["a"] + 1)

    def cap_10(df):
        return df.assign(a=df["a"].clip(0, 10))

    pipeline = Pipeline(
        [
            BatchTransform(
                inc,
                inputs=["a"],
                outputs=["b"],
                chunk_size=CHUNK_SIZE,
            ),
            BatchTransform(
                cap_10,
                inputs=["b"],
                outputs=["a"],
                chunk_size=CHUNK_SIZE,
            ),
        ]
    )

    changeIdx = data_to_index(TEST_DF.loc[[2, 3, 4]], ["id"])
    changelist = ChangeList.create("a", changeIdx)

    catalog.get_datatable(ds, "a").store_chunk(TEST_DF, now=0)

    run_changelist(ds, catalog, pipeline, changelist)

    assert_df_equal(
        catalog.get_datatable(ds, "a").get_data(),
        pd.DataFrame(
            {
                "id": range(10),
                "a": [0, 1, 10, 10, 10, 5, 6, 7, 8, 9],
            }
        ),
    )


def test_run_changelist_with_datatable_transform(dbconn):
    ds = DataStore(dbconn, create_meta_table=True)
    catalog = Catalog(
        {
            "inp": Table(
                store=TableStoreDB(
                    dbconn,
                    "inp_data",
                    TEST_SCHEMA,
                    create_table=True,
                )
            ),
            "out": Table(
                store=TableStoreDB(
                    dbconn,
                    "out_data",
                    TEST_SCHEMA,
                    create_table=True,
                )
            ),
        }
    )

    def transform(
        ds: DataStore,
        input_dts: List[DataTable],
        output_dts: List[DataTable],
        run_config: Optional[RunConfig] = None,
    ):
        input_dt = input_dts[0]
        output_dt = output_dts[0]
        df = input_dt.get_data()
        output_dt.store_chunk(df)

    pipeline = Pipeline(
        [
            DatatableTransform(
                transform,
                inputs=["inp"],
                outputs=["out"],
            ),
        ]
    )

    changeIdx = data_to_index(TEST_DF.loc[[2, 3, 4, 5, 6]], ["id"])
    changelist = ChangeList.create("inp", changeIdx)
    catalog.get_datatable(ds, "inp").store_chunk(TEST_DF, now=0)
    run_changelist(ds, catalog, pipeline, changelist)


def test_magic_injection_variables(dbconn):
    ds = DataStore(dbconn, create_meta_table=True)
    catalog = Catalog(
        {
            "inp": Table(
                store=TableStoreDB(
                    dbconn,
                    "inp_data",
                    TEST_SCHEMA,
                    create_table=True,
                )
            ),
            "out": Table(
                store=TableStoreDB(
                    dbconn,
                    "out_data",
                    TEST_SCHEMA,
                    create_table=True,
                )
            ),
        }
    )
    transform_count = {"value": 0}

    def add_inp_table(ds: DataStore):
        assert isinstance(ds, DataStore)
        transform_count["value"] += 1
        yield TEST_DF

    def transform(df, idx, ds, run_config, transform_count):
        assert isinstance(idx, pd.DataFrame)
        assert isinstance(ds, DataStore)
        assert isinstance(run_config, RunConfig)
        transform_count["value"] += 1
        return df

    pipeline = Pipeline(
        [
            BatchGenerate(func=add_inp_table, outputs=["inp"]),
            BatchTransform(
                transform,
                inputs=["inp"],
                outputs=["out"],
                chunk_size=CHUNK_SIZE,
                kwargs=dict(transform_count=transform_count),
            ),
        ]
    )

    dt_input = catalog.get_datatable(ds, "inp")
    steps = build_compute(ds, catalog, pipeline)
    run_steps(ds, steps)

    dt_out = catalog.get_datatable(ds, "out")
    assert_datatable_equal(dt_out, TEST_DF)

    dt_input.delete_by_idx(dt_input.get_metadata())

    run_steps(ds, steps[1:], RunConfig())
    assert transform_count["value"] == 3


def test_magic_injection_variables_changelist(dbconn):
    ds = DataStore(dbconn, create_meta_table=True)
    catalog = Catalog(
        {
            "inp": Table(
                store=TableStoreDB(
                    dbconn,
                    "inp_data",
                    TEST_SCHEMA,
                    create_table=True,
                )
            ),
            "out": Table(
                store=TableStoreDB(
                    dbconn,
                    "out_data",
                    TEST_SCHEMA,
                    create_table=True,
                )
            ),
        }
    )
    transform_count = {"value": 0}

    def transform(df, idx, ds, run_config, transform_count):
        assert isinstance(idx, pd.DataFrame)
        assert isinstance(ds, DataStore)
        assert isinstance(run_config, RunConfig)
        transform_count["value"] += 1
        return df

    pipeline = Pipeline(
        [
            BatchTransform(
                transform,
                inputs=["inp"],
                outputs=["out"],
                chunk_size=CHUNK_SIZE,
                kwargs=dict(transform_count=transform_count),
            ),
        ]
    )

    dt_input = catalog.get_datatable(ds, "inp")
    change_idx = dt_input.store_chunk(TEST_DF)
    steps = build_compute(ds, catalog, pipeline)
    changelist = ChangeList.create("inp", change_idx)
    run_steps_changelist(ds, steps, changelist, RunConfig())

    dt_out = catalog.get_datatable(ds, "out")
    assert_datatable_equal(dt_out, TEST_DF)

    change_idx = dt_input.get_metadata()
    dt_input.delete_by_idx(dt_input.get_metadata())
    changelist = ChangeList.create("inp", change_idx)
    run_steps_changelist(ds, steps, changelist, RunConfig())
    assert transform_count["value"] == 2


def test_stale_records_with_batch_generate(dbconn):
    ds = DataStore(dbconn, create_meta_table=True)
    catalog = Catalog(
        {
            "inp_keep": Table(
                store=TableStoreDB(
                    dbconn=dbconn,
                    name="inp_data_keep",
                    data_sql_schema=TEST_SCHEMA,
                    create_table=True,
                )
            ),
            "inp_del": Table(
                store=TableStoreDB(
                    dbconn=dbconn,
                    name="inp_data_del",
                    data_sql_schema=TEST_SCHEMA,
                    create_table=True,
                )
            ),
        }
    )

    bg_count = {"value": 0}
    bg_chunk_size = 5

    def add_inp_table(ds: DataStore, bg_count):
        assert isinstance(ds, DataStore)
        bg_count["value"] += 1
        yield pd.DataFrame(
            {
                "id": range(bg_count["value"]*bg_chunk_size, (bg_count["value"]+1) * bg_chunk_size),
                "a": range(bg_count["value"]*bg_chunk_size, (bg_count["value"]+1) * bg_chunk_size),
            }
        )

    pipeline = Pipeline(
        [
            BatchGenerate(
                func=add_inp_table,
                outputs=["inp_keep"],
                delete_stale=False,  # keeps records that are not yielded by func
                kwargs=dict(bg_count=bg_count),
            ),
            BatchGenerate(
                func=add_inp_table,
                outputs=["inp_del"],
                delete_stale=True,  # deletes records that are not yielded by func
                kwargs=dict(bg_count={"value": 0}),  # to avoid double counting
            ),
        ]
    )

    steps = build_compute(ds, catalog, pipeline)

    # First run
    run_steps(ds, steps)

    # Second run
    run_steps(ds, steps)

    # Check table shapes
    df_keep = catalog.get_datatable(ds, "inp_keep").get_data()
    df_del = catalog.get_datatable(ds, "inp_del").get_data()

    assert df_keep.shape[0] == bg_count["value"] * bg_chunk_size
    assert df_del.shape[0] == bg_chunk_size
    # additionally, check that delete_stale=True deletes previous chunks and keeps the last one
    assert df_del.iloc[0]['id'] == bg_chunk_size*bg_count["value"]
