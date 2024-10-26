from datapipe.store.bigquery import BQClient
from datapipe.store.bigquery import TableStoreBQ

import pandas as pd

from sqlalchemy import Column
from sqlalchemy import types


BQ_CREDENTIALS = r"./client_secrets.json"

STORE_NAME = r"test_transformation"
STORE_DATA_SQL_SCHEMA = [
    Column("col_1", types.BIGINT, primary_key=True),
    Column("col_2", types.CHAR),
    Column("col_3", types.BOOLEAN),
]
STORE_DATASET_ID = r"datapipe_test"
STORE_TABLE_ID = r"test"


bq_client = BQClient(service_account_file=BQ_CREDENTIALS)

table_store_bq = TableStoreBQ(
    bq_client=bq_client.bq_client,
    name=STORE_NAME,
    data_sql_schema=STORE_DATA_SQL_SCHEMA,
    dataset_id=STORE_DATASET_ID,
    table_id=STORE_TABLE_ID,
)


df = pd.DataFrame(
    data={
        "col_1": [1, 2, 3],
        "col_2": ["a", "b", "c"],
        "col_3": [False, True, None],
    }
)

print(df)


table_store_bq.insert_rows(df)
