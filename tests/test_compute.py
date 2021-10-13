# from typing import cast
# import pytest

import time
import pandas as pd
from sqlalchemy import Column
from sqlalchemy.sql.sqltypes import Integer

from datapipe.store.database import TableStoreDB
from datapipe.datatable import DataStore
from datapipe.compute import BatchTransformIncStep

# from .util import assert_df_equal, assert_datatable_equal


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


def test_batch_transform(dbconn):
    ds = DataStore(dbconn)

    tbl1 = ds.create_table(
        'tbl1',
        table_store=TableStoreDB(dbconn, 'tbl1_data', TEST_SCHEMA, True)
    )

    tbl2 = ds.create_table(
        'tbl2',
        table_store=TableStoreDB(dbconn, 'tbl2_data', TEST_SCHEMA, True)
    )

    tbl1.store_chunk(TEST_DF, now=0)

    bt_step = BatchTransformIncStep(
        name='tbl1_to_tbl2',
        func=lambda df: df,
        input_dts=[tbl1],
        output_dts=[tbl2],
        chunk_size=1000,
    )

    bt_step.run(ds)

    meta_df = tbl2.get_metadata()

    update_ts = max(meta_df['update_ts'])
    process_ts = max(meta_df['process_ts'])

    time.sleep(0.1)

    bt_step.run(ds)

    meta_df = tbl2.get_metadata()

    assert(all(meta_df['update_ts'] == update_ts))
    assert(all(meta_df['process_ts'] == process_ts))
