import pytest

from datapipe.store.elastic import ElasticStore
from datapipe.store.tests.abstract import AbstractBaseStoreTests
from datapipe.types import DataSchema


@pytest.mark.skip(reason="ElasticStore is temporarily disabled")
class TestElasticStore(AbstractBaseStoreTests):
    @pytest.fixture
    def store_maker(self, elastic_conn):
        def make_elastic_store(data_schema: DataSchema):
            return ElasticStore(
                elastic_conn["index"],
                data_schema,
                elastic_conn["es_kwargs"],
                {},
            )

        return make_elastic_store
