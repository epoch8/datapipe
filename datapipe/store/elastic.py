import base64
import hashlib
from typing import Any, Dict, Iterable, Iterator, List, Optional, TypedDict

import pandas as pd
from elastic_transport import ObjectApiResponse
from elasticsearch import Elasticsearch, helpers
from sqlalchemy import Column

from datapipe.run_config import RunConfig
from datapipe.store.database import MetaKey
from datapipe.store.table_store import TableStore, TableStoreCaps
from datapipe.types import DataDF, DataSchema, IndexDF, MetaSchema


def get_elastic_id(keys: Iterable[Any], length: int = 20) -> str:
    concatenated_keys = "".join([str(key) for key in keys])
    needed_bytes = length * 3 // 4
    hash_object = hashlib.sha256(concatenated_keys.encode("utf-8"))
    hash_bytes = hash_object.digest()[:needed_bytes]
    base64_encoded_id = base64.urlsafe_b64encode(hash_bytes).decode("utf-8")
    return base64_encoded_id[:length]


def _to_itertuples(df: DataDF, colnames):
    return list(df[colnames].itertuples(index=False, name=None))


def remap_dict_keys(data: Dict[str, Any], key_name_remapping: Dict[str, str]) -> Dict[str, Any]:
    return {key_name_remapping.get(key, key): value for key, value in data.items()}


class ElasticStoreState(TypedDict):
    index: str
    data_sql_schema: List[Column]
    es_kwargs: Dict[str, Any]
    key_name_remapping: Optional[Dict[str, str]]
    mapping: Optional[dict]


class ElasticStore(TableStore):
    caps = TableStoreCaps(
        supports_delete=True,
        supports_read_all_rows=True,
        supports_get_schema=True,
        supports_read_meta_pseudo_df=True,
        supports_read_nonexistent_rows=False,
    )

    def __init__(
        self,
        index: str,
        data_sql_schema: List[Column],
        es_kwargs: Dict[str, Any],
        key_name_remapping: Optional[Dict[str, str]] = None,
        mapping: Optional[dict] = None,
    ) -> None:
        self.index = index
        self.data_sql_schema = data_sql_schema
        self.key_name_remapping = key_name_remapping or {}
        self.primary_key_columns = [column.name for column in self.data_sql_schema if column.primary_key]
        self.value_key_columns = [column.name for column in self.data_sql_schema if not column.primary_key]
        self.primary_key_column_rename = "_dtp_orig_{pk}"
        self.mapping = mapping

        self.es_kwargs = es_kwargs
        self.es_client = Elasticsearch(**es_kwargs)

    def __getstate__(self) -> ElasticStoreState:
        return {
            "index": self.index,
            "data_sql_schema": self.data_sql_schema,
            "es_kwargs": self.es_kwargs,
            "mapping": self.mapping,
            "key_name_remapping": self.key_name_remapping,
        }

    def __setstate__(self, state: ElasticStoreState) -> None:
        ElasticStore.__init__(
            self,
            index=state["index"],
            data_sql_schema=state["data_sql_schema"],
            es_kwargs=state["es_kwargs"],
            key_name_remapping=state["key_name_remapping"],
            mapping=state["mapping"],
        )

    def insert_rows(self, df: DataDF) -> None:
        if df.empty:
            return

        # previously index was implicitly created by the bulk api call, now explicit with mapping
        index_exists = self.es_client.indices.exists(index=self.index)
        if not index_exists:
            self.es_client.indices.create(index=self.index, body=self.mapping)

        actions = []
        for row in df.to_dict(orient="records"):  # type: ignore
            # I need to retrieve data in chunks and restore the ids
            # here ids are hashed, so I need to store the original ide values in _source
            # since I cannot store the _id in source (ES will not validate request), I rename these fields
            row_data: Dict[str, Any] = {key: row[key] for key in self.value_key_columns}
            row_id = get_elastic_id([row[key] for key in self.primary_key_columns])
            row_data = remap_dict_keys(row_data, self.key_name_remapping)
            row_data.update(
                {self.primary_key_column_rename.format(pk=key): row[key] for key in self.primary_key_columns}
            )
            actions.append({"_index": self.index, "_source": row_data, "_id": row_id})

        helpers.bulk(client=self.es_client, actions=actions, refresh=True)

    def read_rows(self, idx: Optional[IndexDF] = None) -> DataDF:
        if idx is not None:
            if idx.empty:
                return pd.DataFrame(columns=[column.name for column in self.data_sql_schema])

            key_rows = _to_itertuples(idx, self.primary_key_columns)
            rows_ids = [get_elastic_id(row) for row in key_rows]
            data = self.es_client.mget(index=self.index, body={"ids": rows_ids}, source=True)
            data = data["docs"]
        else:
            # elasticsearch has default limit of 10000 per query
            # I assume you will use the read_rows_meta_pseudo_df for larger result sets
            data = self.es_client.search(index=self.index, query={"match_all": {}}, size=10000)
            data = data["hits"]["hits"]

        remapping_with_primary_keys = {
            **self.key_name_remapping,
            **{
                self.primary_key_column_rename.format(pk=primary_key): f"{primary_key}"
                for primary_key in self.primary_key_columns
            },
        }
        result = [
            remap_dict_keys(item["_source"], remapping_with_primary_keys)  # type: ignore
            for item in data
        ]
        if result:
            return pd.DataFrame(result)
        else:
            return pd.DataFrame(columns=self.primary_key_columns)

    def read_rows_meta_pseudo_df(
        self, chunksize: int = 1000, run_config: Optional[RunConfig] = None
    ) -> Iterator[DataDF]:
        pit_timeout = "5m"

        pit_resp = self.es_client.open_point_in_time(index=self.index, keep_alive=pit_timeout)
        pit_id = pit_resp["id"]

        query: dict
        if run_config:
            # run_config is not taken into account now
            query = {"match_all": {}}
        else:
            query = {"match_all": {}}

        data_resp: Optional[ObjectApiResponse[Any]]
        data_resp = self.es_client.search(
            query=query,
            sort=["_doc"],
            pit={"id": pit_id, "keep_alive": pit_timeout},
            size=chunksize,
        )
        if data_resp and len(data_resp["hits"]["hits"]) == 0:
            data_resp = None
            yield pd.DataFrame(columns=self.primary_key_columns)

        while data_resp:
            data = data_resp["hits"]["hits"]
            last_search_result = data[-1]["sort"]

            remapping_with_primary_keys = {
                **self.key_name_remapping,
                **{
                    self.primary_key_column_rename.format(pk=primary_key): f"{primary_key}"
                    for primary_key in self.primary_key_columns
                },
            }
            result = [remap_dict_keys(item["_source"], remapping_with_primary_keys) for item in data]
            yield pd.DataFrame(result)

            data_resp = self.es_client.search(
                query=query,
                search_after=last_search_result,
                sort=["_doc"],
                pit={"id": pit_id, "keep_alive": pit_timeout},
                size=chunksize,
            )
            if len(data_resp["hits"]["hits"]) == 0:
                data_resp = None

        self.es_client.close_point_in_time(id=pit_id)

    def delete_rows(self, idx: IndexDF) -> None:
        if idx.empty:
            return
        key_rows = _to_itertuples(idx, self.primary_key_columns)
        rows_ids = [get_elastic_id(row) for row in key_rows]
        actions = [{"_op_type": "delete", "_index": self.index, "_id": row_id} for row_id in rows_ids]
        helpers.bulk(client=self.es_client, actions=actions, refresh=True)

    def get_schema(self) -> DataSchema:
        return self.data_sql_schema

    def get_primary_schema(self) -> DataSchema:
        return [column for column in self.data_sql_schema if column.primary_key]

    def get_meta_schema(self) -> MetaSchema:
        meta_key_prop = MetaKey.get_property_name()
        return [column for column in self.data_sql_schema if hasattr(column, meta_key_prop)]
