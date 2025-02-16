import pytest

from datapipe.store.database import DBConn, TableStoreDB
from datapipe.store.tests.abstract import AbstractBaseStoreTests


class TestTableStoreDB(AbstractBaseStoreTests):
    @pytest.fixture
    def store_maker(self, dbconn: DBConn):
        def make_db_store(data_schema):
            return TableStoreDB(
                dbconn=dbconn,
                name="test_table",
                data_sql_schema=data_schema,
                create_table=True,
            )

        return make_db_store
