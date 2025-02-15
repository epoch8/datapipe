import pytest
from sqlalchemy import Column, String

from datapipe.store.redis import RedisStore
from datapipe.store.tests.abstract import (
    AbstractBaseStoreFixtures,
    AbstractBaseStoreTests,
)


class RedisStoreFixtures(AbstractBaseStoreFixtures):
    @pytest.fixture
    def store(self):
        return RedisStore(
            connection="redis://localhost",
            name="test",
            data_sql_schema=[
                Column("id", String(), primary_key=True),
                Column("data", String()),
            ],
        )


class TestBaseRedisStore(AbstractBaseStoreTests, RedisStoreFixtures):
    pass
