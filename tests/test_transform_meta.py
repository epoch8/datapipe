from typing import List

import pytest
from pytest_cases import parametrize
from sqlalchemy import Column, Integer

from datapipe.compute import ComputeInput
from datapipe.datatable import DataTable
from datapipe.event_logger import EventLogger
from datapipe.meta.base import TransformMeta
from datapipe.meta.sql_meta import SQLTableMeta
from datapipe.store.database import DBConn, TableStoreDB
from datapipe.types import MetaSchema


def make_dt(name, dbconn, schema_keys) -> DataTable:
    schema = [Column(key, Integer(), primary_key=True) for key in schema_keys]

    return DataTable(
        name=name,
        meta=SQLTableMeta(
            dbconn=dbconn,
            name=f"{name}_meta",
            primary_schema=schema,
        ),
        table_store=TableStoreDB(
            dbconn=dbconn,
            name=name,
            data_sql_schema=schema,
        ),
        event_logger=EventLogger(),
    )


def assert_schema_equals(sch: MetaSchema, keys: List[str]):
    assert sorted(col.name for col in sch) == sorted(keys)


SUCCESS_CASES = [
    (
        [["a", "b"]],
        [["a", "b"]],
        None,
        ["a", "b"],
    ),
    (
        [["a", "b"]],
        [["x", "b"]],
        None,
        ["b"],
    ),
    (
        [["a", "b"]],
        [["a", "b"]],
        ["a"],
        ["a"],
    ),
    (
        [["bbox_id"], ["model_id"]],
        [["bbox_id", "model_id"]],
        ["bbox_id", "model_id"],
        ["bbox_id", "model_id"],
    ),
]


FAIL_CASES = [
    (
        [["a", "b"]],
        [["x", "y"]],
        None,
    ),
]


@parametrize("input_keys_list,output_keys_list,transform_keys,expected_keys", SUCCESS_CASES)
def test_compute_transform_schema_success(
    dbconn: DBConn,
    input_keys_list,
    output_keys_list,
    transform_keys,
    expected_keys,
):
    inp_cis = [
        ComputeInput(make_dt(f"inp_{i}", dbconn, keys), join_type="full", keys=None)
        for (i, keys) in enumerate(input_keys_list)
    ]
    out_dts = [make_dt(f"out_{i}", dbconn, keys) for (i, keys) in enumerate(output_keys_list)]

    _, sch = TransformMeta.compute_transform_schema(inp_cis, out_dts, transform_keys=transform_keys)

    assert_schema_equals(sch, expected_keys)


@parametrize("input_keys_list,output_keys_list,transform_keys", FAIL_CASES)
def test_compute_transform_schema_fail(
    dbconn: DBConn,
    input_keys_list,
    output_keys_list,
    transform_keys,
):
    inp_cis = [
        ComputeInput(make_dt(f"inp_{i}", dbconn, keys), join_type="full", keys=None)
        for (i, keys) in enumerate(input_keys_list)
    ]
    out_dts = [make_dt(f"out_{i}", dbconn, keys) for (i, keys) in enumerate(output_keys_list)]
    with pytest.raises(AssertionError):
        TransformMeta.compute_transform_schema(inp_cis, out_dts, transform_keys=transform_keys)
