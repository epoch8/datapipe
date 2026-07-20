import pandas as pd
import pytest
from sqlalchemy import Boolean, Column, Integer, String

from datapipe.datatable import DataStore
from datapipe.step.scoped_batch_generate import (
    ScopedOutputPolicy,
    do_scoped_batch_generate,
)
from datapipe.store.database import TableStoreDB
from datapipe.tests.util import assert_df_equal

CONFIG_SCHEMA = [
    Column("detection_train_config_id", String, primary_key=True),
    Column("train_config__source", String),
    Column("train_config__display_name", String),
    Column("train_config__is_active", Boolean),
    Column("value", Integer),
]


def _make_config_table(ds: DataStore, name: str = "yolov8_train_config"):
    return ds.create_table(
        name,
        table_store=TableStoreDB(ds.meta_dbconn, f"{name}_data", CONFIG_SCHEMA, True),
    )


def test_builtin_generation_preserves_custom_rows(dbconn) -> None:
    ds = DataStore(dbconn, create_meta_table=True)
    tbl = _make_config_table(ds)

    tbl.store_chunk(
        pd.DataFrame(
            [
                {
                    "detection_train_config_id": "custom_1",
                    "train_config__source": "custom",
                    "train_config__display_name": "Custom",
                    "train_config__is_active": True,
                    "value": 99,
                },
                {
                    "detection_train_config_id": "builtin_old",
                    "train_config__source": "builtin",
                    "train_config__display_name": "Old",
                    "train_config__is_active": True,
                    "value": 1,
                },
            ]
        )
    )

    def gen():
        yield pd.DataFrame(
            [
                {
                    "detection_train_config_id": "builtin_a",
                    "train_config__source": "builtin",
                    "train_config__display_name": "A",
                    "train_config__is_active": True,
                    "value": 10,
                }
            ]
        )

    do_scoped_batch_generate(
        func=gen,
        ds=ds,
        output_dts=[tbl],
        policy=ScopedOutputPolicy(
            scope_column="train_config__source",
            scope_value="builtin",
            missing_row_policy="deactivate",
            active_column="train_config__is_active",
        ),
    )

    result = tbl.get_data().sort_values("detection_train_config_id").reset_index(drop=True)
    custom = result[result["detection_train_config_id"] == "custom_1"]
    assert len(custom) == 1
    assert bool(custom.iloc[0]["train_config__is_active"]) is True
    assert custom.iloc[0]["value"] == 99

    builtin_a = result[result["detection_train_config_id"] == "builtin_a"]
    assert len(builtin_a) == 1
    assert bool(builtin_a.iloc[0]["train_config__is_active"]) is True


def test_missing_builtin_row_is_deactivated(dbconn) -> None:
    ds = DataStore(dbconn, create_meta_table=True)
    tbl = _make_config_table(ds)

    tbl.store_chunk(
        pd.DataFrame(
            [
                {
                    "detection_train_config_id": "builtin_gone",
                    "train_config__source": "builtin",
                    "train_config__display_name": "Gone",
                    "train_config__is_active": True,
                    "value": 1,
                },
                {
                    "detection_train_config_id": "custom_1",
                    "train_config__source": "custom",
                    "train_config__display_name": "Custom",
                    "train_config__is_active": True,
                    "value": 2,
                },
            ]
        )
    )

    def gen():
        yield pd.DataFrame(
            [
                {
                    "detection_train_config_id": "builtin_keep",
                    "train_config__source": "builtin",
                    "train_config__display_name": "Keep",
                    "train_config__is_active": True,
                    "value": 3,
                }
            ]
        )

    do_scoped_batch_generate(
        func=gen,
        ds=ds,
        output_dts=[tbl],
        policy=ScopedOutputPolicy(
            scope_column="train_config__source",
            scope_value="builtin",
            missing_row_policy="deactivate",
            active_column="train_config__is_active",
        ),
    )

    result = tbl.get_data().set_index("detection_train_config_id")
    assert bool(result.loc["builtin_gone", "train_config__is_active"]) is False
    assert bool(result.loc["builtin_keep", "train_config__is_active"]) is True
    assert bool(result.loc["custom_1", "train_config__is_active"]) is True


def test_generated_builtin_row_is_reactivated(dbconn) -> None:
    ds = DataStore(dbconn, create_meta_table=True)
    tbl = _make_config_table(ds)

    tbl.store_chunk(
        pd.DataFrame(
            [
                {
                    "detection_train_config_id": "builtin_a",
                    "train_config__source": "builtin",
                    "train_config__display_name": "A",
                    "train_config__is_active": False,
                    "value": 1,
                }
            ]
        )
    )

    def gen():
        yield pd.DataFrame(
            [
                {
                    "detection_train_config_id": "builtin_a",
                    "train_config__source": "builtin",
                    "train_config__display_name": "A",
                    "value": 1,
                }
            ]
        )

    do_scoped_batch_generate(
        func=gen,
        ds=ds,
        output_dts=[tbl],
        policy=ScopedOutputPolicy(
            scope_column="train_config__source",
            scope_value="builtin",
            missing_row_policy="deactivate",
            active_column="train_config__is_active",
        ),
    )

    result = tbl.get_data()
    assert len(result) == 1
    assert bool(result.iloc[0]["train_config__is_active"]) is True


def test_scope_mismatch_raises(dbconn) -> None:
    ds = DataStore(dbconn, create_meta_table=True)
    tbl = _make_config_table(ds)

    def gen():
        yield pd.DataFrame(
            [
                {
                    "detection_train_config_id": "custom_bad",
                    "train_config__source": "custom",
                    "train_config__display_name": "Bad",
                    "train_config__is_active": True,
                    "value": 1,
                }
            ]
        )

    with pytest.raises(ValueError, match="mismatched"):
        do_scoped_batch_generate(
            func=gen,
            ds=ds,
            output_dts=[tbl],
            policy=ScopedOutputPolicy(
                scope_column="train_config__source",
                scope_value="builtin",
                missing_row_policy="deactivate",
                active_column="train_config__is_active",
            ),
        )
