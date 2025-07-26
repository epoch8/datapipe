import pandas as pd
from pytest_cases import parametrize
from sqlalchemy import Column, Integer

from datapipe.datatable import DataStore
from datapipe.store.database import TableStoreDB

# from .util import assert_datatable_equal

TEST_SCHEMA = [
    Column("id1", Integer, primary_key=True),
    Column("id2", Integer, primary_key=True),
    Column("a", Integer),
]


@parametrize(
    "N",
    [
        1000,
        5000,
        10000,
        # 100000,
    ],
)
def test_simple(dbconn, N) -> None:
    TEST_DF = pd.DataFrame(
        {
            "id1": range(N),
            "id2": range(N),
            "a": range(N),
        }
    )

    ds = DataStore(dbconn, create_meta_table=True)

    tbl = ds.create_table("test", table_store=TableStoreDB(dbconn, "test_data", TEST_SCHEMA, True))

    tbl.store_chunk(TEST_DF)

    tbl.store_chunk(TEST_DF)

    # TODO fix .get_data() for large datatables
    # assert_datatable_equal(tbl, TEST_DF)
