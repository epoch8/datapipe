import cloudpickle

from datapipe.store.elastic import ElasticStore


def test_cloudpickle() -> None:
    store = ElasticStore(
        index="test_index",
        data_sql_schema=[],
        es_kwargs={"hosts": ["http://localhost:9200"]},
    )
    ser = cloudpickle.dumps(store)

    _ = cloudpickle.loads(ser)
