import os
import numpy as np
import pandas as pd

from sqlalchemy import Column
from sqlalchemy.sql.sqltypes import Integer

from datapipe.datatable import DataStore
from datapipe.compute import build_compute, run_steps
from datapipe.store.pandas import TableStoreJsonLine
from datapipe.store.database import TableStoreDB
from datapipe.compute import Catalog, Pipeline, Table
from datapipe.core_steps import BatchTransform, UpdateExternalTable

from .util import assert_datatable_equal


CHUNK_SIZE = 100

TEST_SCHEMA = [
    Column('id', Integer, primary_key=True),
    Column('a', Integer),
]

TEST_DF = pd.DataFrame(
    {
        'id': range(10),
        'a': range(10),
    },
)


def test_table_store_json_line_reading(tmp_dir, dbconn):
    def conversion(df):
        df["y"] = df["x"] ** 2
        return df

    x = pd.Series(np.arange(2 * CHUNK_SIZE, dtype=np.int32))
    test_df = pd.DataFrame({
        "id": x.apply(str),
        "x": x,
    })
    test_input_fname = os.path.join(tmp_dir, "table-input-pandas.json")
    test_output_fname = os.path.join(tmp_dir, "table-output-pandas.json")
    test_df.to_json(test_input_fname, orient="records", lines=True)

    ds = DataStore(dbconn)
    catalog = Catalog({
        "input_data": Table(
            store=TableStoreJsonLine(test_input_fname),
        ),
        "output_data": Table(
            store=TableStoreJsonLine(test_output_fname),
        ),
    })
    pipeline = Pipeline([
        UpdateExternalTable(
            "input_data"
        ),
        BatchTransform(
            conversion,
            inputs=["input_data"],
            outputs=["output_data"],
            chunk_size=CHUNK_SIZE,
        ),
    ])

    steps = build_compute(ds, catalog, pipeline)
    run_steps(ds, steps)

    df_transformed = catalog.get_datatable(ds, 'output_data').get_data()
    assert len(df_transformed) == 2 * CHUNK_SIZE
    assert all(df_transformed["y"].values == (df_transformed["x"].values ** 2))
    assert len(set(df_transformed["x"].values).symmetric_difference(set(x))) == 0


def test_transform_with_many_input_and_output_tables(tmp_dir, dbconn):
    ds = DataStore(dbconn)
    catalog = Catalog({
        "inp1": Table(
            store=TableStoreDB(
                dbconn,
                'inp1_data',
                TEST_SCHEMA
            )
        ),
        "inp2": Table(
            store=TableStoreDB(
                dbconn,
                'inp2_data',
                TEST_SCHEMA
            )
        ),
        "out1": Table(
            store=TableStoreDB(
                dbconn,
                'out1_data',
                TEST_SCHEMA
            )
        ),
        "out2": Table(
            store=TableStoreDB(
                dbconn,
                'out2_data',
                TEST_SCHEMA
            )
        ),
    })

    def transform(df1, df2):
        return df1, df2

    pipeline = Pipeline([
        BatchTransform(
            transform,
            inputs=["inp1", "inp2"],
            outputs=["out1", "out2"],
            chunk_size=CHUNK_SIZE,
        ),
    ])

    catalog.get_datatable(ds, 'inp1').store_chunk(TEST_DF)
    catalog.get_datatable(ds, 'inp2').store_chunk(TEST_DF)

    steps = build_compute(ds, catalog, pipeline)
    run_steps(ds, steps)

    out1 = catalog.get_datatable(ds, 'out1')
    out2 = catalog.get_datatable(ds, 'out2')

    assert_datatable_equal(out1, TEST_DF)
    assert_datatable_equal(out2, TEST_DF)