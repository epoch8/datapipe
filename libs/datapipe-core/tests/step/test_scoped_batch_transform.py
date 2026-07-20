import pandas as pd
from sqlalchemy import Boolean, Column, Integer, String

from datapipe.compute import ComputeInput, ComputeOutput
from datapipe.datatable import DataStore
from datapipe.step.scoped_batch_generate import ScopedOutputPolicy
from datapipe.step.scoped_batch_transform import ScopedDatatableBatchTransformStep
from datapipe.store.database import TableStoreDB
from datapipe.types import IndexDF

REQUEST_SCHEMA = [
    Column("training_request_id", String, primary_key=True),
    Column("training_request__kind", String),
    Column("training_request__enabled", Boolean),
    Column("frozen_dataset_id", String),
    Column("value", Integer),
]

INPUT_SCHEMA = [
    Column("frozen_dataset_id", String, primary_key=True),
    Column("n", Integer),
]


def test_manual_request_rows_are_not_deleted(dbconn) -> None:
    ds = DataStore(dbconn, create_meta_table=True)

    inp = ds.create_table(
        "frozen_dataset",
        table_store=TableStoreDB(dbconn, "frozen_dataset_data", INPUT_SCHEMA, True),
    )
    out = ds.create_table(
        "training_request",
        table_store=TableStoreDB(dbconn, "training_request_data", REQUEST_SCHEMA, True),
    )

    inp.store_chunk(pd.DataFrame([{"frozen_dataset_id": "ds1", "n": 1}]))
    out.store_chunk(
        pd.DataFrame(
            [
                {
                    "training_request_id": "request_manual_1",
                    "training_request__kind": "manual",
                    "training_request__enabled": True,
                    "frozen_dataset_id": "ds1",
                    "value": 100,
                },
                {
                    "training_request_id": "auto_old",
                    "training_request__kind": "auto",
                    "training_request__enabled": True,
                    "frozen_dataset_id": "ds1",
                    "value": 1,
                },
            ]
        )
    )

    def transform_fn(ds, idx, input_dts, run_config=None, kwargs=None):
        # Replace auto rows with a new auto request; leave manuals alone via scope.
        return pd.DataFrame(
            [
                {
                    "training_request_id": "auto_new",
                    "training_request__kind": "auto",
                    "training_request__enabled": True,
                    "frozen_dataset_id": "ds1",
                    "value": 2,
                }
            ]
        )

    step = ScopedDatatableBatchTransformStep(
        ds=ds,
        name="materialize_auto_requests",
        func=transform_fn,
        input_dts=[ComputeInput(dt=inp, join_type="full")],
        output_dts=[ComputeOutput(dt=out)],
        policy=ScopedOutputPolicy(
            scope_column="training_request__kind",
            scope_value="auto",
            missing_row_policy="deactivate",
            active_column="training_request__enabled",
        ),
        transform_keys=["frozen_dataset_id"],
    )

    step.run_full(ds)

    result = out.get_data().set_index("training_request_id")
    assert "request_manual_1" in result.index
    assert bool(result.loc["request_manual_1", "training_request__enabled"]) is True
    assert result.loc["request_manual_1", "value"] == 100
    assert "auto_new" in result.index
    assert bool(result.loc["auto_new", "training_request__enabled"]) is True


def test_scoped_transform_updates_only_auto_rows(dbconn) -> None:
    ds = DataStore(dbconn, create_meta_table=True)

    inp = ds.create_table(
        "frozen_dataset",
        table_store=TableStoreDB(dbconn, "frozen_dataset_data", INPUT_SCHEMA, True),
    )
    out = ds.create_table(
        "training_request",
        table_store=TableStoreDB(dbconn, "training_request_data", REQUEST_SCHEMA, True),
    )

    inp.store_chunk(
        pd.DataFrame(
            [
                {"frozen_dataset_id": "ds1", "n": 1},
                {"frozen_dataset_id": "ds2", "n": 2},
            ]
        )
    )
    out.store_chunk(
        pd.DataFrame(
            [
                {
                    "training_request_id": "request_manual_1",
                    "training_request__kind": "manual",
                    "training_request__enabled": True,
                    "frozen_dataset_id": "ds1",
                    "value": 100,
                },
                {
                    "training_request_id": "auto_ds1",
                    "training_request__kind": "auto",
                    "training_request__enabled": True,
                    "frozen_dataset_id": "ds1",
                    "value": 1,
                },
                {
                    "training_request_id": "auto_ds2",
                    "training_request__kind": "auto",
                    "training_request__enabled": True,
                    "frozen_dataset_id": "ds2",
                    "value": 2,
                },
            ]
        )
    )

    def transform_fn(ds, idx, input_dts, run_config=None, kwargs=None):
        # Only emit auto row for ds1; ds2 auto should be deactivated for this batch.
        rows = []
        for _, key_row in idx.iterrows():
            if key_row["frozen_dataset_id"] == "ds1":
                rows.append(
                    {
                        "training_request_id": "auto_ds1",
                        "training_request__kind": "auto",
                        "training_request__enabled": True,
                        "frozen_dataset_id": "ds1",
                        "value": 11,
                    }
                )
        return pd.DataFrame(rows) if rows else pd.DataFrame(
            columns=[
                "training_request_id",
                "training_request__kind",
                "training_request__enabled",
                "frozen_dataset_id",
                "value",
            ]
        )

    step = ScopedDatatableBatchTransformStep(
        ds=ds,
        name="materialize_auto_requests",
        func=transform_fn,
        input_dts=[ComputeInput(dt=inp, join_type="full")],
        output_dts=[ComputeOutput(dt=out)],
        policy=ScopedOutputPolicy(
            scope_column="training_request__kind",
            scope_value="auto",
            missing_row_policy="deactivate",
            active_column="training_request__enabled",
        ),
        transform_keys=["frozen_dataset_id"],
        chunk_size=1,
    )

    # Process ds2 with empty generated output → deactivate auto_ds2 only
    step.run_idx(ds, IndexDF(pd.DataFrame([{"frozen_dataset_id": "ds2"}])))

    result = out.get_data().set_index("training_request_id")
    assert bool(result.loc["request_manual_1", "training_request__enabled"]) is True
    assert result.loc["request_manual_1", "value"] == 100
    assert bool(result.loc["auto_ds1", "training_request__enabled"]) is True
    assert result.loc["auto_ds1", "value"] == 1
    assert bool(result.loc["auto_ds2", "training_request__enabled"]) is False

    # Process ds1 → update auto_ds1 value, leave manual untouched
    step.run_idx(ds, IndexDF(pd.DataFrame([{"frozen_dataset_id": "ds1"}])))
    result = out.get_data().set_index("training_request_id")
    assert result.loc["auto_ds1", "value"] == 11
    assert result.loc["request_manual_1", "value"] == 100
