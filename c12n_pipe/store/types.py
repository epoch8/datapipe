from typing import Union, List, Dict, Any

import pandas as pd
from sqlalchemy import Column, String, Numeric, Float

Index = Union[List, pd.Index]
ChunkMeta = Index

DataSchema = List[Column]


def PRIMARY_KEY():
    return [Column('id', String(100), primary_key=True)]


def METADATA_SQL_SCHEMA():
    return [
        Column('hash', Numeric),
        Column('create_ts', Float),  # Время создания строки
        Column('update_ts', Float),  # Время последнего изменения
        Column('process_ts', Float), # Время последней успешной обработки
        Column('delete_ts', Float),  # Время удаления
    ]


