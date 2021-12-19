import os
import numpy as np
import pandas as pd

from datapipe.datatable import DataStore
from datapipe.compute import build_compute, run_steps
from datapipe.store.pandas import TableStoreJsonLine
from datapipe.compute import Catalog, Pipeline, Table
from datapipe.core_steps import BatchTransform, UpdateExternalTable


CHUNK_SIZE = 100


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
