import os
import pandas as pd
from datapipe.store.pandas import TableStoreJsonLine


def test_table_store_json_line_reading(tmp_dir):
    test_df = pd.DataFrame({
        # NOTE если записать сюда числа, даже строками, 
        # Pandas преобразует их в RangeIndex с dtype=int при чтении из JSON
        "id": ["id1", "id2", "id3"],
        "record": ["rec1", "rec2", "rec3"]
    })
    test_fname = os.path.join(tmp_dir, "table-pandas.json")
    test_df.to_json(test_fname, orient="records", lines=True)

    store = TableStoreJsonLine(
        filename=test_fname,
        index_columns=['id']
    )
    df = store.load_file()

    assert all(df.reset_index(drop=False)["id"].values == test_df["id"].values)
    assert all(df["record"].values == test_df["record"].values)
