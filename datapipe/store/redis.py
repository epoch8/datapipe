from distutils.log import warn
from logging import warning
from optparse import Option
from typing import Optional, Union
import warnings

import pandas as pd
from redis.client import Redis
from sqlalchemy import Column, String

from datapipe.store.table_store import TableStore
from datapipe.types import DataDF, DataSchema, MetaSchema, IndexDF, data_to_index


class RedisStore(TableStore):
    def __init__(
        self,
        # ВОПРОС: какие типы соединений мы хотим поддерживать? Пока не трогаем pydantic, но по-хорошему наверное стоит.
        connection: Union[Redis, str],
        primary_schema: Optional[DataSchema] = None
    ) -> None:

        if isinstance(connection, str):
            self.redis_connection = Redis.from_url(connection, decode_responses=True)
        else:
            self.redis_connection = connection
        
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

        self.primary_key = [column.name for column in self.primary_schema if column.primary_key]
        if not self.primary_key:
            raise RuntimeError('Primary key for Redis store not specified.')
        elif len(self.primary_key) > 1:
            raise RuntimeError('Multiple primary keys for Redis store are not supported.')
        else:
            self.primary_key = self.primary_key[0]
            self.value_col = [column.name for column in self.primary_schema if not column.primary_key][0]

    def insert_rows(self, df: DataDF) -> None:
        if df.empty:
            return
        # ВОПРОС: Тут нужна проверка на дубликаты, или она делается где-то уровнями выше?
        if df[self.primary_key].duplicated().any():
            raise ValueError("DataDF contains duplicate primary keys.")

        # Редис будет ругаться на пандасовые типы, поэтому сколлапсим все в стрингу,
        # при чтении конвертируем обратно в соответствии со схемой
        df = df.astype(str) 
        # Заливка через пайплайн должна работать быстрей для большого кол-ва ключей.
        pipeline = self.redis_connection.pipeline()
        for key, value in zip(df[self.primary_key], df[self.value_col]):
            pipeline.set(key, value)
        pipeline.execute()
    
    def update_rows(self, df: DataDF) -> None:
        # удаляем существующие ключи
        self.delete_rows(data_to_index(df, self.primary_key))
        self.insert_rows(df)

    def read_rows(self, keys: Optional[IndexDF] = None) -> DataDF:
        # без ключей читаем всю базу
        if keys is None:
            keys = self.redis_connection.keys()
            values = self.redis_connection.mget(keys)
        else:
            keys = keys[self.primary_key].astype(str).to_list()
            values = self.redis_connection.mget(keys)
        
        # проверяем кодирование
        if keys:
            if isinstance(keys[0], bytes):
                keys = [k.decode() for k in keys]
            if isinstance(values[0], bytes):
                values = [v.decode() for v in values]
        
        result_df = pd.DataFrame({self.primary_key: keys, self.value_col: values})
        # приводим типы к схеме
        dtypes = {column.name: column.type.python_type for column in self.primary_schema}
        result_df = result_df.astype(dtypes)
        return result_df

    def delete_rows(self, keys: IndexDF) -> None:
        # как всегда конверсия
        if keys.empty:
            return
        keys = keys[self.primary_key].astype(str).to_list()
        self.redis_connection.delete(*keys)
    
    def get_primary_schema(self) -> DataSchema:
        return self.primary_schema
    
    def get_meta_schema(self) -> MetaSchema:
        return []