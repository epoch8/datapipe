import cloudpickle
from sqlalchemy import Column, String

from datapipe.store.redis import RedisStore

def test_redis_cloudpickle():
    store = RedisStore(
        connection='redis://localhost',
        name='test',
        data_sql_schema=[
            Column("id", String(), primary_key=True),
            Column("data", String()),
        ]
    )

    ser = cloudpickle.dumps(store)

    deser = cloudpickle.loads(ser)
