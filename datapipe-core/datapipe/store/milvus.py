from typing import Dict, List, Optional

import pandas as pd
from pymilvus import (
    Collection,
    CollectionSchema,
    FieldSchema,
    SearchResult,
    connections,
    utility,
)

from datapipe.store.table_store import TableStore
from datapipe.types import DataDF, DataSchema, IndexDF, MetaSchema, data_to_index


class MilvusStore(TableStore):
    def __init__(
        self,
        name: str,
        schema: List[FieldSchema],
        primary_db_schema: DataSchema,
        index_params: Dict,
        pk_field: str,
        embedding_field: str,
        connection_details: Dict,
    ):
        super().__init__()
        self.name = name
        self.schema = schema
        self.primary_db_schema = primary_db_schema
        self.index_params = index_params
        self.pk_field = pk_field
        self.embedding_field = embedding_field
        self.connection_details = connection_details
        self._collection_loaded = False

        connections.connect(**connection_details)

        schema_milvus = CollectionSchema(schema, "MilvusStore")
        self.collection = Collection(name, schema_milvus)

        if not utility.has_collection(name):
            self.collection.create_index(embedding_field, index_params)

    def get_primary_schema(self) -> DataSchema:
        return self.primary_db_schema

    def get_meta_schema(self) -> MetaSchema:
        return []

    def pk_expr(self, idx: IndexDF) -> str:
        if isinstance(idx[self.pk_field].values[0], str):
            values = idx[self.pk_field].apply(lambda val: f"'{val}'")
        else:
            values = idx[self.pk_field].apply(str)

        return ", ".join(values)

    def delete_rows(self, idx: IndexDF) -> None:
        if len(idx) == 0:
            return

        ids_joined = self.pk_expr(idx)

        self.collection.delete(expr=f"{self.pk_field} in [{ids_joined}]")

        if self._collection_loaded:
            self.collection.release()
            self._collection_loaded = False

    def insert_rows(self, df: DataDF) -> None:
        if len(df) == 0:
            return

        values = [df[field.name].tolist() for field in self.schema]

        self.collection.insert(values)

        if self._collection_loaded:
            self.collection.release()
            self._collection_loaded = False

    def update_rows(self, df: DataDF) -> None:
        self.delete_rows(data_to_index(df, [self.pk_field]))
        self.insert_rows(df)

    def read_rows(self, idx: Optional[IndexDF] = None) -> DataDF:
        if not idx:
            raise Exception("Milvus doesn't support full store reading")

        ids_joined = self.pk_expr(idx)
        names = [field.name for field in self.schema]

        result = self.query_search(f"{self.pk_field} in [{ids_joined}]", names)

        return pd.DataFrame.from_records(result)

    def vector_search(
        self, embeddings: List, query_params: Dict, expr: str, limit: int
    ) -> SearchResult:
        if not self._collection_loaded:
            self.collection.load()
            self._collection_loaded = True

        return self.collection.search(
            data=embeddings,
            anns_field=self.embedding_field,
            param={"params": query_params},
            limit=limit,
            expr=expr,
            consistency_level="Strong",
        )

    def query_search(self, expr: str, output_fields: List) -> List:
        if not self._collection_loaded:
            self.collection.load()
            self._collection_loaded = True

        return self.collection.query(expr=expr, output_fields=output_fields)
