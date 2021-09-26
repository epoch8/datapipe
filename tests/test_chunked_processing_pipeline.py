import os
import numpy as np
import pandas as pd
from datapipe.compute import build_compute, run_steps
from datapipe.store.pandas import TableStoreJsonLine
from datapipe.metastore import MetaStore
from datapipe.dsl import Catalog, ExternalTable, Pipeline, BatchTransform, Table


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

    ms = MetaStore(dbconn)
    catalog = Catalog({
        "input_data": ExternalTable(
            store=TableStoreJsonLine(test_input_fname),
        ),
        "output_data": Table(
            store=TableStoreJsonLine(test_output_fname),
        ),
    })
    pipeline = Pipeline([
        BatchTransform(
            conversion,
            inputs=["input_data"],
            outputs=["output_data"],
            chunk_size=CHUNK_SIZE,
        ),
    ])

    steps = build_compute(ms, catalog, pipeline)
    run_steps(ms, steps)

    df_transformed = catalog.get_datatable(ms, 'output_data').get_data()
    assert len(df_transformed) == 2 * CHUNK_SIZE
    assert all(df_transformed["y"].values == (df_transformed["x"].values ** 2))
    assert len(set(df_transformed["x"].values).symmetric_difference(set(x))) == 0
