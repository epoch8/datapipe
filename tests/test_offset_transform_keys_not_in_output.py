"""
Test for offset update when output doesn't contain all transform_keys.

Reproduces issue where transform_keys include columns that are aggregated away
in the output, causing KeyError when trying to extract processed_idx from output.
"""

import time

import pandas as pd
from sqlalchemy import Column, String

from datapipe.compute import ComputeInput
from datapipe.datatable import DataStore
from datapipe.step.batch_transform import BatchTransformStep
from datapipe.store.database import DBConn, TableStoreDB


def test_offset_update_does_not_require_transform_keys_in_output(dbconn: DBConn) -> None:
    ds = DataStore(dbconn, create_meta_table=True)

    input_store = TableStoreDB(
        dbconn,
        "items",
        [
            Column("id", String, primary_key=True),
            Column("group_id", String, primary_key=True),
            Column("creator_profile_id", String, primary_key=True),
            Column("status", String),
        ],
        create_table=True,
    )
    input_dt = ds.create_table("items", input_store)

    output_store = TableStoreDB(
        dbconn,
        "group_status",
        [
            Column("group_id", String, primary_key=True),
            Column("status", String),
        ],
        create_table=True,
    )
    output_dt = ds.create_table("group_status", output_store)

    def aggregate(df_items: pd.DataFrame) -> pd.DataFrame:
        # Return aggregated output WITHOUT id/creator_profile_id (this is intentional).
        return (
            df_items.groupby("group_id", as_index=False)
            .agg({"status": "first"})
            .reset_index(drop=True)
        )

    step = BatchTransformStep(
        ds=ds,
        name="aggregate_group_status",
        func=aggregate,
        input_dts=[ComputeInput(dt=input_dt, join_type="full")],
        output_dts=[output_dt],
        transform_keys=["id", "group_id", "creator_profile_id"],
        use_offset_optimization=True,
        chunk_size=10,
    )

    t1 = time.time()
    input_dt.store_chunk(
        pd.DataFrame(
            [
                {
                    "id": "p1",
                    "group_id": "c1",
                    "creator_profile_id": "u1",
                    "status": "on_moderation",
                }
            ]
        ),
        now=t1,
    )

    # Before the fix this crashed with:
    # KeyError: "['id', 'creator_profile_id'] not in index"
    step.run_full(ds)

    out_df = output_dt.get_data()
    assert len(out_df) == 1
    assert out_df.iloc[0]["group_id"] == "c1"

    # Offset must be updated for the input table.
    offset = ds.offset_table.get_offset(step.get_name(), "items")
    assert offset is not None
