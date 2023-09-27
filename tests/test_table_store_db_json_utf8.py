import pandas as pd
from datapipe.store.database import TableStoreDB
from sqlalchemy import Column, String, JSON


DF_TEST = pd.DataFrame({"id": [0], "data": ["тест abc предложение def"]})


def test_table_store_db_json_utf8(dbconn):
    table_store = TableStoreDB(
        dbconn,
        "tbl",
        [
            Column("id", String),
            Column("data", JSON),
        ],
        create_table=True,
    )
    table_store.insert_rows(DF_TEST)
    df = table_store.read_rows()
    assert df.iloc[0]["data"]["value"] == DF_TEST.iloc[0]["data"]["value"]
