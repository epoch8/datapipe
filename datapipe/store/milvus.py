from typing import List, Union
import numpy as np
import pandas as pd
from pymilvus import connections, utility, CollectionSchema, Collection, FieldSchema, SearchResult
from sqlalchemy.sql.schema import Column
from datapipe.store.table_store import TableStore
from datapipe.types import IndexDF, DataDF, DataSchema, MetaSchema


class MilvusStore(TableStore):
    def __init__(self, name: str, 
                 schema: List[FieldSchema],
                 primary_db_schema: List[Column], 
                 index_params: dict, 
                 pk_field: str, 
                 embedding_field: str, 
                 connection_details: dict):
        """
        MILVUS_SCHEMA_OZON = [
            FieldSchema(name="id_numeric", dtype=DataType.INT64, is_primary=True, auto_id=False),
            FieldSchema(name="category_id", dtype=DataType.INT64),
            FieldSchema(name="embedding", dtype=DataType.FLOAT_VECTOR, dim=768),
            FieldSchema(name="search_space", dtype=DataType.INT64),
        ]
        MILVUS_PRIMARY_SCHEMA_OZON = [
            Column("id_numeric", BIGINT(), primary_key=True),
        ]
        catalog = Catalog({
            "tmp__ozon_edit_ozon_embeddings": Table(
                store=MilvusStore(
                    name="tmp__ozon",
                    schema=MILVUS_SCHEMA_OZON,
                    primary_db_schema=MILVUS_PRIMARY_SCHEMA_OZON,
                    index_params={"index_type": "HNSW", "params": {"M": 32, "efConstruction": 40}, "metric_type": "L2"},
                    pk_field="id_numeric",
                    embedding_field="embedding",
                    connection_details={
                        "alias": "default",
                        "host": "milvus.milvus.svc.cluster.local",
                        "port": "19530"
                    }
                )
            ),
        })
        """
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
        if not utility.has_collection(name):
            self._create_collection(name, schema, index_params, embedding_field)
        schema_milvus = CollectionSchema(schema, "MilvusStore")
        self.collection = Collection(name, schema_milvus)
        
    def get_primary_schema(self) -> List[Column]:
        return self.primary_db_schema

    def get_meta_schema(self) -> List[Column]:
        return []
        
    def pk_expr(self, idx: IndexDF) -> str:
        if isinstance(idx[self.pk_field].values[0], str):
            values = idx[self.pk_field].apply(lambda val: f"'{val}'")
        else:
            values = idx[self.pk_field].apply(str)
        ids_joined = ", ".join(values)
        return ids_joined
        
    def _create_collection(self, name: str, schema: List[FieldSchema], index_params: dict, embedding_field: str) -> None:
        schema_milvus = CollectionSchema(schema, "MilvusStore")
        collection = Collection(name, schema_milvus)
        collection.create_index(embedding_field, index_params)
    
    def delete_rows(self, idx: IndexDF) -> None:
        if len(idx) == 0:
            return
        ids_joined = self.pk_expr(idx)
        query = f"{self.pk_field} in [{ids_joined}]"
        self.collection.delete(expr=query)
        if self._collection_loaded:
            self.collection.release()
            self._collection_loaded = False
    
    def insert_rows(self, df: DataDF) -> None:
        if len(df) == 0:
            return
        names = [item.name for item in self.schema]
        values = [df[name].tolist() for name in names]
        self.collection.insert(values)
        if self._collection_loaded:
            self.collection.release()
            self._collection_loaded = False
        
    def update_rows(self, df: DataDF) -> None:
        self.delete_rows(df[[self.pk_field]])
        self.insert_rows(df)
    
    def read_rows(self, idx: IndexDF) -> DataDF:
        ids_joined = self.pk_expr(idx)
        query = f"{self.pk_field} in [{ids_joined}]"
        names = [field.name for field in self.schema]
        result = self.query_search(query, names)
        return pd.DataFrame.from_records(result)
        
    def vector_search(self, embeddings: Union[List[List[float]], np.ndarray], 
                      query_params: dict, expr: str, limit: int) -> SearchResult:
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
    
    def query_search(self, expr: str, output_fields: List[str]) -> List[dict]:
        if not self._collection_loaded:
            self.collection.load()
            self._collection_loaded = True
        return self.collection.query(
            expr=expr,
            output_fields=output_fields
        )
