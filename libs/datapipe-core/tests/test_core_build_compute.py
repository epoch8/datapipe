import pandas as pd
from sqlalchemy import Column
from sqlalchemy.sql.sqltypes import String

from datapipe.compute import Catalog, Pipeline, Table, build_compute
from datapipe.datatable import DataStore
from datapipe.step.batch_generate import BatchGenerate
from datapipe.step.batch_transform import BatchTransform, DatatableBatchTransform
from datapipe.step.datatable_transform import DatatableTransform
from datapipe.step.update_external_table import UpdateExternalTable
from datapipe.store.database import TableStoreDB


def test_batch_transform_step_name_is_stable(dbconn):
    ds = DataStore(dbconn, create_meta_table=True)
    catalog = Catalog(
        {
            "item": Table(store=TableStoreDB(dbconn, "item", [Column("item_id", String, primary_key=True)], True)),
            "pipeline": Table(
                store=TableStoreDB(dbconn, "pipeline", [Column("pipeline_id", String, primary_key=True)], True)
            ),
            "prediction": Table(
                store=TableStoreDB(
                    dbconn,
                    "prediction",
                    [Column("item_id", String, primary_key=True), Column("pipeline_id", String, primary_key=True)],
                    True,
                )
            ),
            "keypoint": Table(
                store=TableStoreDB(dbconn, "keypoint", [Column("keypoint_id", String, primary_key=True)], True)
            ),
            "output": Table(
                store=TableStoreDB(
                    dbconn,
                    "output",
                    [Column("item_id", String, primary_key=True), Column("pipeline_id", String, primary_key=True)],
                    True,
                )
            ),
        }
    )

    def complex_function(df__item, df__pipeline, df__prediction, df__keypoint):
        return pd.DataFrame()

    pipeline = Pipeline(
        [
            BatchTransform(
                func=complex_function,
                inputs=["item", "pipeline", "prediction", "keypoint"],
                outputs=["output"],
                transform_keys=["item_id", "pipeline_id"],
            ),
        ]
    )
    steps = build_compute(ds, catalog, pipeline)

    assert steps[0].name == "complex_function_9762dd6bae"


def _make_simple_catalog(dbconn, tables: list[str]) -> Catalog:
    return Catalog(
        {
            name: Table(
                store=TableStoreDB(
                    dbconn,
                    name,
                    [Column(f"{name}_id", String, primary_key=True)],
                    True,
                )
            )
            for name in tables
        }
    )


def test_datatable_batch_transform_step_name_is_stable(dbconn):
    ds = DataStore(dbconn, create_meta_table=True)
    catalog = Catalog(
        {
            "item": Table(
                store=TableStoreDB(
                    dbconn,
                    "item",
                    [Column("item_id", String, primary_key=True)],
                    True,
                )
            ),
            "output": Table(
                store=TableStoreDB(
                    dbconn,
                    "output",
                    [Column("item_id", String, primary_key=True)],
                    True,
                )
            ),
        }
    )

    def dt_batch_func(ds, idx, input_dts, run_config=None, kwargs=None):  # noqa: ARG001
        return input_dts[0].get_data(idx)

    pipeline = Pipeline(
        [
            DatatableBatchTransform(
                func=dt_batch_func,
                inputs=["item"],
                outputs=["output"],
            ),
        ]
    )
    steps = build_compute(ds, catalog, pipeline)

    assert steps[0].name == "dt_batch_func_c4bf4c0307"


def test_batch_generate_step_name_is_stable(dbconn):
    ds = DataStore(dbconn, create_meta_table=True)
    catalog = _make_simple_catalog(dbconn, ["output"])

    def batch_gen_func():
        yield pd.DataFrame({"output_id": ["a"]})

    pipeline = Pipeline(
        [
            BatchGenerate(
                func=batch_gen_func,
                outputs=["output"],
            ),
        ]
    )
    steps = build_compute(ds, catalog, pipeline)

    assert steps[0].name == "batch_gen_func_e48d04b448"


def test_datatable_transform_step_name_is_stable(dbconn):
    ds = DataStore(dbconn, create_meta_table=True)
    catalog = _make_simple_catalog(dbconn, ["item", "output"])

    def dt_transform_func(ds, input_dts, output_dts, run_config, kwargs=None):
        pass

    dt_transform_func.__name__ = "dt_transform_func"

    pipeline = Pipeline(
        [
            DatatableTransform(
                func=dt_transform_func,
                inputs=["item"],
                outputs=["output"],
            ),
        ]
    )
    steps = build_compute(ds, catalog, pipeline)

    assert steps[0].name == "dt_transform_func_212f5d0a5d"


def test_update_external_table_step_name_is_stable(dbconn):
    ds = DataStore(dbconn, create_meta_table=True)
    catalog = _make_simple_catalog(dbconn, ["item"])

    pipeline = Pipeline(
        [
            UpdateExternalTable(output="item"),
        ]
    )
    steps = build_compute(ds, catalog, pipeline)

    assert steps[0].name == "update_item_75abd0d249"
