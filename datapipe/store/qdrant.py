import hashlib
import re
import uuid
from collections.abc import Iterable
from typing import Any, Dict, List, Optional, cast

import pandas as pd
from qdrant_client import QdrantClient
from qdrant_client.http import models as rest
from qdrant_client.http.exceptions import UnexpectedResponse

from datapipe.store.table_store import TableStore
from datapipe.types import DataDF, DataSchema, IndexDF, MetaSchema


class CollectionParams(rest.CreateCollection):
    pass


class QdrantStore(TableStore):
    """Defines a TableStore for piping data to Qdrant collection

    Args:
        name (str): name of the Qdrant collection
        url (str): url of the Qdrant server (if using with api_key,
        you should explicitly specify port 443, by default qdrant uses 6333)
        schema (DataSchema): Describes data that will be stored in the Qdrant collection
        pk_field (str): name of the primary key field in the schema, used to identify records
        embedding_field (str): name of the field in the schema that contains the vector representation of the record
        collection_params (CollectionParams): parameters for creating a collection in Qdrant
        api_key (Optional[str]): api_key for Qdrant server
    """

    def __init__(
        self,
        name: str,
        url: str,
        schema: DataSchema,
        pk_field: str,
        embedding_field: str,
        collection_params: CollectionParams,
        api_key: Optional[str] = None,
    ):
        super().__init__()
        self.name = name
        self.url = url
        self.schema = schema
        self.pk_field = pk_field
        self.embedding_field = embedding_field
        self.collection_params = collection_params
        self.inited = False
        self.client: Optional[QdrantClient] = None
        self._api_key = api_key

        pk_columns = [column for column in self.schema if column.primary_key]

        if len(pk_columns) != 1 and pk_columns[0].name != pk_field:
            raise ValueError("Incorrect prymary key columns in schema")

        self.paylods_filelds = [
            column.name for column in self.schema if column.name != self.embedding_field
        ]

    def __init(self):
        self.client = QdrantClient(url=self.url, api_key=self._api_key)
        try:
            self.client.get_collection(self.name)
        except UnexpectedResponse as e:
            if e.status_code == 404:
                self.client.http.collections_api.create_collection(
                    collection_name=self.name, create_collection=self.collection_params
                )

    def __check_init(self):
        if not self.inited:
            self.__init()
            self.inited = True

    def __get_ids(self, df):
        return (
            df[self.pk_field]
            .apply(
                lambda x: str(
                    uuid.UUID(bytes=hashlib.md5(str(x).encode("utf-8")).digest())
                )
            )
            .to_list()
        )

    def get_primary_schema(self) -> DataSchema:
        return [column for column in self.schema if column.primary_key]

    def get_meta_schema(self) -> MetaSchema:
        return []

    def insert_rows(self, df: DataDF) -> None:
        self.__check_init()

        if len(df) == 0:
            return

        assert self.client is not None
        self.client.upsert(
            self.name,
            rest.Batch(
                ids=self.__get_ids(df),
                vectors=df[self.embedding_field].apply(list).to_list(),
                payloads=cast(
                    List[Dict[str, Any]],
                    df[self.paylods_filelds].to_dict(orient="records"),
                ),
            ),
            wait=True,
        )

    def update_rows(self, df: DataDF) -> None:
        self.insert_rows(df)

    def delete_rows(self, idx: IndexDF) -> None:
        self.__check_init()

        if len(idx) == 0:
            return

        assert self.client is not None
        self.client.delete(
            self.name,
            rest.PointIdsList(points=self.__get_ids(idx)),
            wait=True,
        )

    def read_rows(self, idx: Optional[IndexDF] = None) -> DataDF:
        self.__check_init()

        if idx is None:
            raise Exception("Qrand doesn't support full store reading")

        assert self.client is not None
        response = self.client.http.points_api.get_points(
            self.name,
            point_request=rest.PointRequest(
                ids=self.__get_ids(idx), with_payload=True, with_vector=True
            ),
        )

        records = []

        assert response.result is not None
        for point in response.result:
            record = point.payload

            assert record is not None
            record[self.embedding_field] = point.vector

            records.append(record)

        return pd.DataFrame.from_records(records)[
            [column.name for column in self.schema]
        ]


class QdrantShardedStore(TableStore):
    """Defines a TableStore for piping data to multiple Qdrant collections

    Args:
        name_pattern (str): name pattern of the Qdrant collections
        url (str): url of the Qdrant server (if using with api_key,
        you should explicitly specify port 443, by default qdrant uses 6333)
        schema (DataSchema): Describes data that will be stored in the Qdrant collection
        embedding_field (str): name of the field in the schema that contains the vector representation of the record
        collection_params (CollectionParams): parameters for creating a collection in Qdrant
        api_key (Optional[str]): api_key for Qdrant server
    """

    def __init__(
        self,
        name_pattern: str,
        url: str,
        schema: DataSchema,
        embedding_field: str,
        collection_params: CollectionParams,
        api_key: Optional[str] = None,
    ):
        super().__init__()
        self.name_pattern = name_pattern
        self.url = url
        self.schema = schema
        self.embedding_field = embedding_field
        self.collection_params = collection_params
        self._api_key = api_key

        self.inited_collections: set = set()
        self.client: Optional[QdrantClient] = None

        self.pk_fields = [column.name for column in self.schema if column.primary_key]
        self.paylods_filelds = [
            column.name for column in self.schema if column.name != self.embedding_field
        ]
        self.name_params = re.findall(r"\{([^/]+?)\}", self.name_pattern)

        if not len(self.pk_fields):
            raise ValueError("Prymary key columns not found in schema")

        if not self.name_params or set(self.name_params) - set(self.pk_fields):
            raise ValueError("Incorrect params in name pattern")

    def __init_collection(self, name):
        try:
            self.client.get_collection(name)
        except UnexpectedResponse as e:
            if e.status_code == 404:
                self.client.http.collections_api.create_collection(
                    collection_name=name, create_collection=self.collection_params
                )

    def __check_init(self, name):
        if not self.client:
            self.client = QdrantClient(url=self.url, api_key=self._api_key)

        if name not in self.inited_collections:
            self.__init_collection(name)
            self.inited_collections.add(name)

    def __get_ids(self, df):
        ids_values = (
            df[self.pk_fields]
            .apply(lambda x: "_".join([f"{i[0]}-{i[1]}" for i in x.items()]), axis=1)
            .to_list()
        )

        return list(
            map(
                lambda x: str(
                    uuid.UUID(bytes=hashlib.md5(str(x).encode("utf-8")).digest())
                ),
                ids_values,
            )
        )

    def get_primary_schema(self) -> DataSchema:
        return [column for column in self.schema if column.primary_key]

    def get_meta_schema(self) -> MetaSchema:
        return []

    def __get_collection_name(self, name_values: Any) -> str:
        if len(self.name_params) == 1:
            name_values = (name_values,)

        name_params = dict(zip(self.name_params, name_values))
        return self.name_pattern.format(**name_params)

    def insert_rows(self, df: DataDF) -> None:
        for name_values, gdf in df.groupby(by=self.name_params):
            name = self.__get_collection_name(name_values)

            self.__check_init(name)

            assert self.client is not None
            self.client.upsert(
                name,
                rest.Batch(
                    ids=self.__get_ids(gdf),
                    vectors=gdf[self.embedding_field].apply(list).to_list(),
                    payloads=cast(
                        List[Dict[str, Any]],
                        df[self.paylods_filelds].to_dict(orient="records"),
                    ),
                ),
                wait=True,
            )

    def update_rows(self, df: DataDF) -> None:
        self.insert_rows(df)

    def delete_rows(self, idx: IndexDF) -> None:
        for name_val, gdf in idx.groupby(by=self.name_params):
            name = self.__get_collection_name(name_val)

            self.__check_init(name)

            assert self.client is not None
            self.client.delete(
                name,
                rest.PointIdsList(points=self.__get_ids(gdf)),
                wait=True,
            )

    def read_rows(self, idx: Optional[IndexDF] = None) -> DataDF:
        if not idx:
            raise Exception("Qrand doesn't support full store reading")

        records = []

        for name_val, gdf in idx.groupby(by=self.name_params):
            name = self.__get_collection_name(name_val)

            self.__check_init(name)

            assert self.client is not None
            response = self.client.http.points_api.get_points(
                name,
                point_request=rest.PointRequest(
                    ids=self.__get_ids(gdf), with_payload=True, with_vector=True
                ),
            )

            assert response.result is not None
            for point in response.result:
                record = point.payload

                assert record is not None
                record[self.embedding_field] = point.vector

                records.append(record)

        return pd.DataFrame.from_records(records)[
            [column.name for column in self.schema]
        ]
