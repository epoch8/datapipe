from typing import List

import pytest
from pytest_cases import parametrize
from sqlalchemy import Column, Integer

from datapipe.metastore import MetaTable
from datapipe.step.batch_transform import BatchTransformStep
from datapipe.store.database import DBConn
from datapipe.types import MetaSchema


def make_mt(name, dbconn, schema_keys) -> MetaTable:
    return MetaTable(
        dbconn=dbconn,
        name=name,
        primary_schema=[
            Column(key, Integer(), primary_key=True) for key in schema_keys
        ],
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


@parametrize(
    "input_keys_list,output_keys_list,transform_keys,expected_keys", SUCCESS_CASES
)
def test_compute_transform_schema_success(
    dbconn: DBConn,
    input_keys_list,
    output_keys_list,
    transform_keys,
    expected_keys,
):
    inp_mts = [
        make_mt(f"inp_{i}", dbconn, keys) for (i, keys) in enumerate(input_keys_list)
    ]
    out_mts = [
        make_mt(f"out_{i}", dbconn, keys) for (i, keys) in enumerate(output_keys_list)
    ]

    _, sch = BatchTransformStep.compute_transform_schema(
        inp_mts, out_mts, transform_keys=transform_keys
    )

    assert_schema_equals(sch, expected_keys)


@parametrize("input_keys_list,output_keys_list,transform_keys", FAIL_CASES)
def test_compute_transform_schema_fail(
    dbconn: DBConn,
    input_keys_list,
    output_keys_list,
    transform_keys,
):
    inp_mts = [
        make_mt(f"inp_{i}", dbconn, keys) for (i, keys) in enumerate(input_keys_list)
    ]
    out_mts = [
        make_mt(f"out_{i}", dbconn, keys) for (i, keys) in enumerate(output_keys_list)
    ]

    with pytest.raises(AssertionError):
        BatchTransformStep.compute_transform_schema(
            inp_mts, out_mts, transform_keys=transform_keys
        )
