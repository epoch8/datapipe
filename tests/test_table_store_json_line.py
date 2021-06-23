import os
import pandas as pd

from datapipe.store.pandas import TableStoreJsonLine
from .util import tmp_dir


def test_table_store_json_line_reading(tmp_dir):
    test_df = pd.DataFrame({
        "id": ["0", "1", "2"],
        "record": ["rec1", "rec2", "rec3"]
    })
    test_fname = os.path.join(tmp_dir, "table-pandas.json")
    test_df.to_json(test_fname, orient="records", lines=True)

    store = TableStoreJsonLine(
        filename=test_fname
    )
    df = store.load_file()
    assert all(df.reset_index(drop=False)["id"].values == test_df["id"].values)
    assert all(df["record"].values == test_df["record"].values)
