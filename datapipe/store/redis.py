import json
from typing import Dict, List, Optional, Union

import pandas as pd
from redis.client import Redis
from redis.cluster import RedisCluster, ClusterNode
from sqlalchemy import Column

from datapipe.store.database import MetaKey
from datapipe.store.table_store import TableStore, TableStoreCaps
from datapipe.types import DataDF, DataSchema, IndexDF, MetaSchema, data_to_index


def _serialize(values):
    return json.dumps(values)


def _deserialize(bytestring):
    return json.loads(bytestring)


def _to_itertuples(df: DataDF, colnames):
    return list(df[colnames].itertuples(index=False, name=None))


def _parse_cluster_hosts(hosts):
    nodes = []
    for host in hosts.split(","):
        host, port = host.split(":")
        nodes.append(ClusterNode(host, port))
    return nodes


class RedisStore(TableStore):
    caps = TableStoreCaps(
        supports_delete=True,
        supports_get_schema=False,
        supports_read_all_rows=False,
        supports_read_nonexistent_rows=False,  # TODO check
        supports_read_meta_pseudo_df=False,
    )

    def __init__(
        self,
        connection: str,
        name: str,
        data_sql_schema: List[Column],
        cluster_mode: bool = False,
        password: Optional[str] = None,
    ) -> None:
        self.connection = connection
        if not cluster_mode:
            self.redis_connection: Union[Redis, RedisCluster] = Redis.from_url(connection, decode_responses=True)
        else:
            if "," in connection:
                self.redis_connection = RedisCluster(
                    startup_nodes=_parse_cluster_hosts(connection.removeprefix("redis://")),
                    password=password,
                    decode_responses=True
                )
            else:
                self.redis_connection = RedisCluster.from_url(connection, decode_responses=True)

        self.name = name
        self.data_sql_schema = data_sql_schema
        self.prim_keys = [column.name for column in self.data_sql_schema if column.primary_key]
        self.value_cols = [column.name for column in self.data_sql_schema if not column.primary_key]

    def __getstate__(self) -> Dict:
        return {
            "connection": self.connection,
            "name": self.name,
            "data_sql_schema": self.data_sql_schema,
        }

    def __setstate__(self, state: Dict):
        RedisStore.__init__(
            self,
            connection=state["connection"],
            name=state["name"],
            data_sql_schema=state["data_sql_schema"],
        )

    def insert_rows(self, df: DataDF) -> None:
        if df.empty:
            return

        # get rows as Iter[Tuple]
        key_rows = _to_itertuples(df, self.prim_keys)
        value_rows = _to_itertuples(df, self.value_cols)
        redis_pipe = self.redis_connection.pipeline()
        for keys, values in zip(key_rows, value_rows):
            redis_pipe.hset(self.name, _serialize(keys), _serialize(values))
        redis_pipe.execute()

    def update_rows(self, df: DataDF) -> None:
        # удаляем существующие ключи
        if df.empty:
            df = pd.DataFrame(columns=[column.name for column in self.data_sql_schema])
        self.delete_rows(data_to_index(df, self.prim_keys))
        self.insert_rows(df)

    def read_rows(self, df_keys: Optional[IndexDF] = None) -> DataDF:
        assert df_keys is not None

        if df_keys.empty:
            return pd.DataFrame(columns=[column.name for column in self.data_sql_schema])

        keys = _to_itertuples(df_keys, self.prim_keys)
        keys_json = [_serialize(key) for key in keys]
        values = self.redis_connection.hmget(self.name, keys_json)
        data = [list(key) + _deserialize(val) for key, val in zip(keys, values) if val]

        result_df = pd.DataFrame.from_records(data, columns=self.prim_keys + self.value_cols)
        return result_df

    def delete_rows(self, df_keys: IndexDF) -> None:
        if df_keys.empty:
            return
        keys = _to_itertuples(df_keys, self.prim_keys)
        keys = [_serialize(key) for key in keys]
        self.redis_connection.hdel(self.name, *keys)

    def get_primary_schema(self) -> DataSchema:
        return [column for column in self.data_sql_schema if column.primary_key]

    def get_meta_schema(self) -> MetaSchema:
        meta_key_prop = MetaKey.get_property_name()
        return [column for column in self.data_sql_schema if hasattr(column, meta_key_prop)]
