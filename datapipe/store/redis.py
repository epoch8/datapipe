from logging import warning
from typing import Optional, Union
import json

import pandas as pd
from redis.client import Redis
from sqlalchemy import Column, String

from datapipe.store.table_store import TableStore
from datapipe.types import DataDF, DataSchema, MetaSchema, IndexDF, data_to_index


def _serialize(dict):
    return json.dumps(dict)


def _deserialize(bytes):
    return json.loads(bytes)


class RedisStore(TableStore):
    def __init__(
        self,
        connection: Union[Redis, str],
        name: str,
        primary_schema: Optional[DataSchema] = None
    ) -> None:

        if isinstance(connection, str):
            print(connection)
            self.redis_connection = Redis.from_url(connection, decode_responses=True)
        else:
            self.redis_connection = connection
        self.name = name

        if primary_schema is None:
            self.primary_schema = [
                Column("id", String(), primary_key=True),
                Column("value", String())
            ]
            warning.warn(
                '''
                    Primary schema for redis store is not specified.
                    Input data must contain fields "id" for keys and "value" for values.
                '''
            )
        else:
            self.primary_schema = primary_schema

        self.prim_keys = [column.name for column in self.primary_schema if column.primary_key]
        # Вопрос: нам нужна эта проверка?
        if not self.prim_keys:
            raise RuntimeError('Primary key for Redis store not specified.')

        self.value_cols = [column.name for column in self.primary_schema if not column.primary_key]

    def insert_rows(self, df: DataDF) -> None:
        if df.empty:
            return
        # ВОПРОС: Как делать проверку на дубликаты для множественных primary_key?
        # key pairs as dict {'key_1: key_1_val, 'key_2': key_2_val}
        key_rows = df[self.prim_keys].to_dict(orient='records')
        value_rows = df[self.value_cols].to_dict(orient='records')
        redis_pipe = self.redis_connection.pipeline()
        for keys, values in zip(key_rows, value_rows):
            redis_pipe.hset(self.name, _serialize(keys), _serialize(values))
        redis_pipe.execute()

    def update_rows(self, df: DataDF) -> None:
        # удаляем существующие ключи
        self.delete_rows(data_to_index(df, self.prim_keys))
        self.insert_rows(df)

    def read_rows(self, keys: Optional[IndexDF] = None) -> DataDF:
        # без ключей читаем всю базу
        if keys is None:
            pairs = self.redis_connection.hgetall(self.name)
            keys = [_deserialize(key) for key in pairs.keys()]
            values = [_deserialize(val) for val in pairs.values()]
        else:
            keys = keys[self.prim_keys].to_dict(orient='records')
            keys_json = [_serialize(key) for key in keys]
            values = self.redis_connection.hmget(self.name, keys_json) if keys_json else []
            values = [_deserialize(val) for val in values]

        result_df = pd.concat([pd.DataFrame(keys), pd.DataFrame(values)], axis=1)
        return result_df

    def delete_rows(self, keys: IndexDF) -> None:
        if keys.empty:
            return
        keys = keys[self.prim_keys].to_dict(orient='records')
        keys = [_serialize(key) for key in keys]
        self.redis_connection.hdel(self.name, *keys)

    def get_primary_schema(self) -> DataSchema:
        return self.primary_schema

    def get_meta_schema(self) -> MetaSchema:
        return []
