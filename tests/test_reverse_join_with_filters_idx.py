"""
Test for ambiguous column error fix in reverse join with filters_idx.

Reproduces the bug where using join_keys with filters_idx caused SQL errors:
"psycopg2.errors.AmbiguousColumn: column reference 'id' is ambiguous"

The issue occurred because sql_apply_filters_idx_to_subquery used sa.column(i)
without table qualification, which failed for JOIN queries with multiple tables
sharing the same column names.
"""

import time

import pandas as pd
from sqlalchemy import Column, String

from datapipe.compute import ComputeInput
from datapipe.datatable import DataStore
from datapipe.step.batch_transform import BatchTransformStep
from datapipe.store.database import DBConn, TableStoreDB
from datapipe.types import ChangeList


def test_reverse_join_with_filters_idx_no_ambiguous_column(dbconn: DBConn):
    """
    Test that reverse join with filters_idx doesn't cause ambiguous column errors.

    Scenario:
    1. Create two tables: campaigns (primary) and moderation (reference with join_keys)
    2. Both tables have common columns: id, ad_campaign_id
    3. Use join_keys for reverse join: {"id": "id", "ad_campaign_id": "ad_campaign_id"}
    4. Run changelist with filters_idx containing these column names
    5. Verify SQL executes without "ambiguous column" error
    """
    ds = DataStore(dbconn, create_meta_table=True)

    # 1. Create campaigns table (primary)
    campaigns_store = TableStoreDB(
        dbconn,
        "campaigns",
        [
            Column("id", String, primary_key=True),
            Column("ad_campaign_id", String, primary_key=True),
            Column("status", String),
        ],
        create_table=True,
    )
    campaigns_dt = ds.create_table("campaigns", campaigns_store)

    # 2. Create moderation table (reference with join_keys)
    moderation_store = TableStoreDB(
        dbconn,
        "moderation",
        [
            Column("id", String, primary_key=True),
            Column("ad_campaign_id", String, primary_key=True),
            Column("approved", String),
        ],
        create_table=True,
    )
    moderation_dt = ds.create_table("moderation", moderation_store)

    # 3. Create output table
    output_store = TableStoreDB(
        dbconn,
        "aggregated",
        [
            Column("id", String, primary_key=True),
            Column("ad_campaign_id", String, primary_key=True),
            Column("final_status", String),
        ],
        create_table=True,
    )
    output_dt = ds.create_table("aggregated", output_store)

    # 4. Define transformation with join_keys
    def transform_func(campaigns_df, moderation_df):
        """LEFT JOIN campaigns with moderation results."""
        result = campaigns_df.merge(
            moderation_df,
            on=["id", "ad_campaign_id"],
            how="left",
        )
        result["final_status"] = result.apply(
            lambda row: row["approved"] if pd.notna(row["approved"]) else row["status"],
            axis=1
        )
        return result[["id", "ad_campaign_id", "final_status"]]

    step = BatchTransformStep(
        ds=ds,
        name="test_transform",
        func=transform_func,
        input_dts=[
            ComputeInput(dt=campaigns_dt, join_type="full"),
            # CRITICAL: join_keys with columns that exist in both tables
            ComputeInput(
                dt=moderation_dt,
                join_type="full",
                join_keys={"id": "id", "ad_campaign_id": "ad_campaign_id"}
            ),
        ],
        output_dts=[output_dt],
        transform_keys=["id", "ad_campaign_id"],
        use_offset_optimization=True,
        chunk_size=10,
    )

    # === INITIAL DATA ===
    t1 = time.time()

    # Create campaign
    campaigns_df = pd.DataFrame([
        {"id": "post_1", "ad_campaign_id": "camp_1", "status": "pending"},
    ])
    campaigns_dt.store_chunk(campaigns_df, now=t1)

    # === FIRST RUN ===
    time.sleep(0.01)
    step.run_full(ds)

    output_data = output_dt.get_data()
    assert len(output_data) == 1, f"Expected 1 record in output, got {len(output_data)}"
    assert output_data.iloc[0]["final_status"] == "pending"

    # === ADD MODERATION RESULT ===
    time.sleep(0.01)
    t2 = time.time()

    # Add moderation approval
    moderation_df = pd.DataFrame([
        {"id": "post_1", "ad_campaign_id": "camp_1", "approved": "approved"},
    ])
    moderation_dt.store_chunk(moderation_df, now=t2)

    # === SECOND RUN WITH FILTERS_IDX ===
    # This is where the ambiguous column error would occur before the fix
    time.sleep(0.01)

    # Create changelist with specific idx (simulating webhook update)
    cl = ChangeList()
    cl.append(
        "moderation",
        pd.DataFrame([{"id": "post_1", "ad_campaign_id": "camp_1"}])
    )

    # Run with changelist - this will use filters_idx internally
    # Before the fix, this would fail with:
    # "psycopg2.errors.AmbiguousColumn: column reference 'id' is ambiguous"
    step.run_changelist(ds, cl)

    # === VERIFY ===
    output_data_after = output_dt.get_data()
    assert len(output_data_after) == 1
    assert output_data_after.iloc[0]["final_status"] == "approved", \
        f"Expected status 'approved', got '{output_data_after.iloc[0]['final_status']}'"

    print("✓ Test passed: reverse join with filters_idx works without ambiguous column error")


def test_multiple_tables_with_shared_columns_and_filters(dbconn: DBConn):
    """
    Test more complex scenario with 3 tables sharing column names.

    This ensures the fix works even with multiple JOINs.
    """
    ds = DataStore(dbconn, create_meta_table=True)

    # Create 3 tables with shared "id" column
    table1_store = TableStoreDB(
        dbconn, "table1",
        [Column("id", String, primary_key=True), Column("value1", String)],
        create_table=True,
    )
    table1_dt = ds.create_table("table1", table1_store)

    table2_store = TableStoreDB(
        dbconn, "table2",
        [Column("id", String, primary_key=True), Column("value2", String)],
        create_table=True,
    )
    table2_dt = ds.create_table("table2", table2_store)

    table3_store = TableStoreDB(
        dbconn, "table3",
        [Column("id", String, primary_key=True), Column("value3", String)],
        create_table=True,
    )
    table3_dt = ds.create_table("table3", table3_store)

    output_store = TableStoreDB(
        dbconn, "output",
        [
            Column("id", String, primary_key=True),
            Column("combined", String),
        ],
        create_table=True,
    )
    output_dt = ds.create_table("output", output_store)

    def transform_func(df1, df2, df3):
        result = df1.merge(df2, on="id", how="left")
        result = result.merge(df3, on="id", how="left")
        result["combined"] = (
            result["value1"].fillna("") + "_" +
            result["value2"].fillna("") + "_" +
            result["value3"].fillna("")
        )
        return result[["id", "combined"]]

    step = BatchTransformStep(
        ds=ds,
        name="test_multi_join",
        func=transform_func,
        input_dts=[
            ComputeInput(dt=table1_dt, join_type="full"),
            ComputeInput(dt=table2_dt, join_type="full", join_keys={"id": "id"}),
            ComputeInput(dt=table3_dt, join_type="full", join_keys={"id": "id"}),
        ],
        output_dts=[output_dt],
        transform_keys=["id"],
        use_offset_optimization=True,
        chunk_size=10,
    )

    t1 = time.time()
    table1_dt.store_chunk(pd.DataFrame([{"id": "1", "value1": "a"}]), now=t1)
    table2_dt.store_chunk(pd.DataFrame([{"id": "1", "value2": "b"}]), now=t1)

    time.sleep(0.01)
    step.run_full(ds)

    # Update table3 and run with changelist
    time.sleep(0.01)
    t2 = time.time()
    table3_dt.store_chunk(pd.DataFrame([{"id": "1", "value3": "c"}]), now=t2)

    time.sleep(0.01)
    cl = ChangeList()
    cl.append("table3", pd.DataFrame([{"id": "1"}]))

    # This should work without ambiguous column error
    step.run_changelist(ds, cl)

    output_data = output_dt.get_data()
    assert len(output_data) == 1
    assert output_data.iloc[0]["combined"] == "a_b_c"

    print("✓ Test passed: multiple tables with shared columns and filters work correctly")


def test_production_case_advertising_campaign_moderation(dbconn: DBConn):
    """
    Test real production scenario that caused ambiguous column errors.

    Reproduces the exact structure from production:
    - advertising_campaign_to_moderate (primary)
    - advertising_campaign_moderation_manual_separated (join_keys: id + ad_campaign_id)
    - advertising_campaign_manual_overrides (join_keys: id only)

    This scenario has:
    - Multiple tables with overlapping columns (id, ad_campaign_id)
    - Mixed join_keys (composite vs single)
    - transform_keys spanning multiple tables
    """
    ds = DataStore(dbconn, create_meta_table=True)

    # 1. Primary table: campaigns to moderate
    campaign_to_moderate_store = TableStoreDB(
        dbconn,
        "advertising_campaign_to_moderate",
        [
            Column("id", String, primary_key=True),
            Column("ad_campaign_id", String, primary_key=True),
            Column("creator_profile_id", String, primary_key=True),
            Column("status", String),
        ],
        create_table=True,
    )
    campaign_to_moderate_dt = ds.create_table(
        "advertising_campaign_to_moderate", campaign_to_moderate_store
    )

    # 2. Manual moderation (reverse join with composite key: id + ad_campaign_id)
    moderation_manual_store = TableStoreDB(
        dbconn,
        "advertising_campaign_moderation_manual_separated",
        [
            Column("id", String, primary_key=True),
            Column("ad_campaign_id", String, primary_key=True),
            Column("moderation_result", String),
        ],
        create_table=True,
    )
    moderation_manual_dt = ds.create_table(
        "advertising_campaign_moderation_manual_separated", moderation_manual_store
    )

    # 3. Manual overrides (reverse join with single key: id only)
    manual_overrides_store = TableStoreDB(
        dbconn,
        "advertising_campaign_manual_overrides",
        [
            Column("id", String, primary_key=True),
            Column("override_status", String),
        ],
        create_table=True,
    )
    manual_overrides_dt = ds.create_table(
        "advertising_campaign_manual_overrides", manual_overrides_store
    )

    # 4. Output table
    output_store = TableStoreDB(
        dbconn,
        "advertising_campaign_moderation_aggregated",
        [
            Column("id", String, primary_key=True),
            Column("ad_campaign_id", String, primary_key=True),
            Column("creator_profile_id", String, primary_key=True),
            Column("final_status", String),
        ],
        create_table=True,
    )
    output_dt = ds.create_table("advertising_campaign_moderation_aggregated", output_store)

    # 5. Transformation logic (similar to real production)
    def transform_func(campaigns_df, moderation_df, overrides_df):
        """Aggregate moderation results with manual overrides."""
        # Join with manual moderation
        result = campaigns_df.merge(
            moderation_df,
            on=["id", "ad_campaign_id"],
            how="left",
        )

        # Join with manual overrides
        result = result.merge(
            overrides_df,
            on="id",
            how="left",
        )

        # Apply logic: override_status > moderation_result > status
        def get_final_status(row):
            if pd.notna(row.get("override_status")):
                return row["override_status"]
            elif pd.notna(row.get("moderation_result")):
                return row["moderation_result"]
            else:
                return row["status"]

        result["final_status"] = result.apply(get_final_status, axis=1)

        return result[["id", "ad_campaign_id", "creator_profile_id", "final_status"]]

    # CRITICAL: This is the exact configuration that caused the bug
    step = BatchTransformStep(
        ds=ds,
        name="advertising_campaign_moderation_aggregation",
        func=transform_func,
        input_dts=[
            ComputeInput(dt=campaign_to_moderate_dt, join_type="full"),
            # Composite join_keys: id + ad_campaign_id
            ComputeInput(
                dt=moderation_manual_dt,
                join_type="full",
                join_keys={"id": "id", "ad_campaign_id": "ad_campaign_id"},
            ),
            # Single join_key: id only
            ComputeInput(
                dt=manual_overrides_dt,
                join_type="full",
                join_keys={"id": "id"},
            ),
        ],
        output_dts=[output_dt],
        transform_keys=["id", "ad_campaign_id", "creator_profile_id"],
        use_offset_optimization=True,
        chunk_size=10,
    )

    # === INITIAL DATA ===
    t1 = time.time()

    # Create campaign
    campaigns_df = pd.DataFrame([
        {
            "id": "post_1",
            "ad_campaign_id": "camp_1",
            "creator_profile_id": "creator_1",
            "status": "pending",
        },
    ])
    campaign_to_moderate_dt.store_chunk(campaigns_df, now=t1)

    # === FIRST RUN ===
    time.sleep(0.01)
    step.run_full(ds)

    output_data = output_dt.get_data()
    assert len(output_data) == 1
    assert output_data.iloc[0]["final_status"] == "pending"

    # === ADD MANUAL MODERATION ===
    time.sleep(0.01)
    t2 = time.time()

    moderation_df = pd.DataFrame([
        {
            "id": "post_1",
            "ad_campaign_id": "camp_1",
            "moderation_result": "approved",
        },
    ])
    moderation_manual_dt.store_chunk(moderation_df, now=t2)

    # === RUN WITH CHANGELIST - This caused ambiguous column error before fix ===
    time.sleep(0.01)
    cl = ChangeList()
    cl.append(
        "advertising_campaign_moderation_manual_separated",
        pd.DataFrame([{"id": "post_1", "ad_campaign_id": "camp_1"}])
    )

    # Before fix: psycopg2.errors.AmbiguousColumn: column reference 'id' is ambiguous
    step.run_changelist(ds, cl)

    # === VERIFY ===
    output_data_after = output_dt.get_data()
    assert len(output_data_after) == 1
    assert output_data_after.iloc[0]["final_status"] == "approved"

    # === ADD MANUAL OVERRIDE ===
    time.sleep(0.01)
    t3 = time.time()

    overrides_df = pd.DataFrame([
        {
            "id": "post_1",
            "override_status": "rejected",
        },
    ])
    manual_overrides_dt.store_chunk(overrides_df, now=t3)

    # === RUN WITH CHANGELIST FOR OVERRIDE ===
    time.sleep(0.01)
    cl2 = ChangeList()
    cl2.append(
        "advertising_campaign_manual_overrides",
        pd.DataFrame([{"id": "post_1"}])
    )

    step.run_changelist(ds, cl2)

    # === FINAL VERIFICATION ===
    output_data_final = output_dt.get_data()
    assert len(output_data_final) == 1
    assert output_data_final.iloc[0]["final_status"] == "rejected", \
        "Override should take precedence"

    print("✓ Test passed: production case with composite join_keys works without ambiguous column error")
