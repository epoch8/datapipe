import pandas as pd
from sqlalchemy import Column
from sqlalchemy.sql.sqltypes import String

from datapipe.compute import Catalog, Pipeline, Table, build_compute, run_steps
from datapipe.datatable import DataStore
from datapipe.step.batch_transform import BatchTransform
from datapipe.store.database import TableStoreDB
from datapipe.tests.util import assert_datatable_equal, assert_idx_no_duplicates

TEST__ITEM = pd.DataFrame(
    {
        "item_id": [f"item_id{i}" for i in range(10)],
        "item__attribute": [f"item__attribute{i}" for i in range(10)],
    }
)

TEST__ITEM2 = pd.DataFrame(
    {
        "item_id": [f"item_id{i}" for i in range(100)],
        "item__attribute": [f"item__attribute{i}" for i in range(100)],
    }
)

TEST__PIPELINE = pd.DataFrame(
    {
        "pipeline_id": [f"pipeline_id{i}" for i in range(3)],
        "pipeline__attribute": [f"pipeline_attribute{i}" for i in range(3)],
    }
)


def test_check_full_step_idx_count_when_some_idxs_are_deleted(dbconn):
    ds = DataStore(dbconn, create_meta_table=True)
    catalog = Catalog(
        {
            "item": Table(
                store=TableStoreDB(
                    dbconn,
                    "item",
                    [
                        Column("item_id", String, primary_key=True),
                        Column("item__attribute", String),
                    ],
                    True,
                )
            ),
            "item2": Table(
                store=TableStoreDB(
                    dbconn,
                    "item2",
                    [
                        Column("item_id", String, primary_key=True),
                        Column("item__attribute", String),
                    ],
                    True,
                )
            ),
            "pipeline": Table(
                store=TableStoreDB(
                    dbconn,
                    "pipeline",
                    [
                        Column("pipeline_id", String, primary_key=True),
                        Column("pipeline__attribute", String),
                    ],
                    True,
                )
            ),
            "output": Table(
                store=TableStoreDB(
                    dbconn,
                    "output",
                    [
                        Column("item_id", String, primary_key=True),
                        Column("pipeline_id", String, primary_key=True),
                    ],
                    True,
                )
            ),
        }
    )

    def complex_function(
        df__item, df__item2, df__pipeline
    ):
        df__item = pd.merge(df__item, df__item2)
        df__output = pd.merge(df__item[["item_id"]], df__pipeline[["pipeline_id"]], how='cross')
        return df__output[["item_id", "pipeline_id"]]

    pipeline = Pipeline(
        [
            BatchTransform(
                func=complex_function,
                inputs=["item", "item2", "pipeline"],
                outputs=["output"],
                transform_keys=["item_id", "pipeline_id"],
                chunk_size=50,
            ),
        ]
    )
    steps = build_compute(ds, catalog, pipeline)
    step = steps[-1]
    ds.get_table("item").store_chunk(TEST__ITEM)
    ds.get_table("item2").store_chunk(TEST__ITEM2)
    ds.get_table("pipeline").store_chunk(TEST__PIPELINE)
    ds.get_table("item").delete_by_idx(TEST__ITEM.iloc[0:5])
    ds.get_table("item2").delete_by_idx(TEST__ITEM.iloc[0:5])
    count, idx_gen = step.get_full_process_ids(ds)
    idx = pd.concat([idx for idx in idx_gen], ignore_index=True)
    assert_idx_no_duplicates(idx, ["item_id", "pipeline_id"])
    TEST_RESULT = complex_function(
        TEST__ITEM.iloc[5:],
        TEST__ITEM2.iloc[5:],
        TEST__PIPELINE,
    )
    run_steps(ds, steps)
    assert_datatable_equal(ds.get_table("output"), TEST_RESULT)
