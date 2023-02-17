from typing import List, Callable

from sqlalchemy import Column, DateTime, Integer


class MetaMixin:
    created_at = Column(DateTime())
    updated_at = Column(DateTime())
    hash = Column(Integer())


class Pipeline:
    def __init__(self, base):
        self.base = base

    def transform(self, ids: List | None = None) -> Callable:
        raise NotImplementedError()

    def transform_sql(self, ids: List | None = None) -> Callable:
        raise NotImplementedError()

    def transform_pandas(self, ids: List | None = None, batch: int = 1000) -> Callable:
        raise NotImplementedError()


class QdrantStore:
    def connect(self):
        raise NotImplementedError()


def compile_to_prefect(pipeline: Pipeline):
    raise NotImplementedError
