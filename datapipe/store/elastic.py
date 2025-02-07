import base64
import hashlib
import typing as t

import pandas as pd
from datapipe.store.database import MetaKey
from datapipe.store.table_store import TableStore
from datapipe.types import DataDF, DataSchema, IndexDF, MetaSchema
from elasticsearch import Elasticsearch, helpers
from sqlalchemy import Column


def get_elastic_id(keys: t.Iterable[t.Any], length: int = 20) -> str:
    concatenated_keys = "".join([str(key) for key in keys])
    needed_bytes = length * 3 // 4
    hash_object = hashlib.sha256(concatenated_keys.encode("utf-8"))
    hash_bytes = hash_object.digest()[:needed_bytes]
    base64_encoded_id = base64.urlsafe_b64encode(hash_bytes).decode("utf-8")
    return base64_encoded_id[:length]


def _to_itertuples(df: DataDF, colnames):
    return list(df[colnames].itertuples(index=False, name=None))


def remap_dict_keys(data: t.Dict[str, t.Any], key_name_remapping: t.Dict[str, str]) -> t.Dict[str, t.Any]:
    return {key_name_remapping.get(key, key): value for key, value in data.items()}


class ElasticStoreState(t.TypedDict):
    index: str
    data_sql_schema: t.List[Column]
    es_kwargs: t.Dict[str, t.Any]
    key_name_remapping: t.Optional[t.Dict[str, str]]


class ElasticStore(TableStore):
    def __init__(
        self,
        index: str,
        data_sql_schema: t.List[Column],
        es_kwargs: t.Dict[str, t.Any],
        key_name_remapping: t.Optional[t.Dict[str, str]] = None,
    ) -> None:
        self.index = index
        self.data_sql_schema = data_sql_schema
        self.key_name_remapping = key_name_remapping or {}
        self.primary_key_columns = [column.name for column in self.data_sql_schema if column.primary_key]
        self.value_key_columns = [column.name for column in self.data_sql_schema if not column.primary_key]

        self.es_kwargs = es_kwargs
        self.es_client = Elasticsearch(**es_kwargs)

    def __getstate__(self) -> ElasticStoreState:
        return {
            "index": self.index,
            "data_sql_schema": self.data_sql_schema,
            "es_kwargs": self.es_kwargs,
            "key_name_remapping": self.key_name_remapping,
        }

    def __setstate__(self, state: ElasticStoreState) -> None:
        ElasticStore.__init__(
            self,
            index=state["index"],
            data_sql_schema=state["data_sql_schema"],
            es_kwargs=state["es_kwargs"],
            key_name_remapping=state["key_name_remapping"],
        )

    def insert_rows(self, df: DataDF) -> None:
        if df.empty:
            return

        actions = []
        for row in df.to_dict(orient="records"):  # type: ignore
            row_data: t.Dict[str, t.Any] = {key: row[key] for key in self.value_key_columns + self.primary_key_columns}
            row_id = get_elastic_id([row_data[key] for key in self.primary_key_columns])
            row_data = remap_dict_keys(row_data, self.key_name_remapping)
            actions.append({"_index": self.index, "_source": row_data, "_id": row_id})

        helpers.bulk(client=self.es_client, actions=actions, refresh=True)

    def read_rows(self, idx: t.Optional[IndexDF] = None) -> DataDF:
        assert idx is not None, "No keys provided for read_rows"

        if idx.empty:
            return pd.DataFrame(columns=[column.name for column in self.data_sql_schema])

        key_rows = _to_itertuples(idx, self.primary_key_columns)
        rows_ids = [get_elastic_id(row) for row in key_rows]
        data = self.es_client.mget(index=self.index, body={"ids": rows_ids}, source=True)
        result = [remap_dict_keys(item["_source"], self.key_name_remapping) for item in data["docs"]]
        return pd.DataFrame(result)

    def delete_rows(self, idx: IndexDF) -> None:
        if idx.empty:
            return
        key_rows = _to_itertuples(idx, self.primary_key_columns)
        rows_ids = [get_elastic_id(row) for row in key_rows]
        actions = [{"_op_type": "delete", "_index": self.index, "_id": row_id} for row_id in rows_ids]
        helpers.bulk(client=self.es_client, actions=actions, refresh=True)

    def get_primary_schema(self) -> DataSchema:
        return [column for column in self.data_sql_schema if column.primary_key]

    def get_meta_schema(self) -> MetaSchema:
        meta_key_prop = MetaKey.get_property_name()
        return [column for column in self.data_sql_schema if hasattr(column, meta_key_prop)]
