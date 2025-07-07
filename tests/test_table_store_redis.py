import pytest

from datapipe.store.redis import RedisStore
from datapipe.store.tests.abstract import AbstractBaseStoreTests
from datapipe.types import DataSchema


class TestRedisStore(AbstractBaseStoreTests):
    @pytest.fixture
    def store_maker(self):
        def make_redis_store(data_schema: DataSchema):
            return RedisStore(
                connection="redis://localhost",
                name="test",
                data_sql_schema=data_schema,
            )

        return make_redis_store
