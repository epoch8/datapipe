from typing import List, Any, Dict, Union, Optional, Iterator

import copy
import logging

import pandas as pd
from opentelemetry import trace
from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor

from sqlalchemy import Column, Table, create_engine, MetaData, String, Integer
from sqlalchemy.sql.expression import select, delete, tuple_
from sqlalchemy.pool import SingletonThreadPool
from sqlalchemy.schema import SchemaItem
from sqlalchemy.sql.base import SchemaEventTarget

from datapipe.run_config import RunConfig
from datapipe.types import DataDF, IndexDF, DataSchema, MetaSchema, data_to_index
from datapipe.store.table_store import TableStore


logger = logging.getLogger("datapipe.store.database")
tracer = trace.get_tracer("datapipe.store.database")


SCHEMA_TO_DTYPE_LOOKUP = {
    String: str,
    Integer: int,
}


def sql_schema_to_dtype(schema: List[Column]) -> Dict[str, Any]:
    return {
        i.name: SCHEMA_TO_DTYPE_LOOKUP[i.type.__class__]
        for i in schema
    }


def sql_schema_to_sqltype(schema: List[Column]) -> Dict[str, Any]:
    return {
        i.name: i.type for i in schema
    }


class DBConn:
    def __init__(self, connstr: str, schema: str = None):
        self._init(connstr, schema)

    def _init(self, connstr: str, schema: Optional[str]) -> None:
        self.connstr = connstr
        self.schema = schema

        if connstr.startswith('sqlite'):
            self.supports_update_from = False
        else:
            # Assume relatively new Postgres
            self.supports_update_from = True

        self.con = create_engine(
            connstr,
            poolclass=SingletonThreadPool,
        )

        SQLAlchemyInstrumentor().instrument(
            engine=self.con
        )

        self.sqla_metadata = MetaData(schema=schema)

    def __getstate__(self):
        return {
            'connstr': self.connstr,
            'schema': self.schema,
        }

    def __setstate__(self, state):
        self._init(state['connstr'], state['schema'])


def sql_apply_runconfig_filter(sql: select, table: Table, primary_keys: List[str], run_config: RunConfig = None) -> select:
    if run_config is not None:
        for k, v in run_config.filters.items():
            if k in primary_keys:
                sql = sql.where(
                    table.c[k] == v
                )

    return sql


class MetaKey(SchemaItem):
    def __init__(self, target_name: str = None) -> None:
        self.target_name = target_name

    def _set_parent(self, parent: SchemaEventTarget, **kw: Any) -> None:
        self.parent = parent
        self.parent.meta_key = self

        if not self.target_name:
            self.target_name = parent.name

    @classmethod
    def get_property_name(cls) -> str:
        return "meta_key"


class TableStoreDB(TableStore):
    def __init__(
        self,
        dbconn: Union['DBConn', str],
        name: str,
        data_sql_schema: List[Column],
        create_table: bool = False,
    ) -> None:
        if isinstance(dbconn, str):
            self.dbconn = DBConn(dbconn)
        else:
            self.dbconn = dbconn
        self.name = name

        self.data_sql_schema = data_sql_schema

        self.data_table = Table(
            self.name, self.dbconn.sqla_metadata,
            *[copy.copy(i) for i in self.data_sql_schema],
            extend_existing=True
        )

        if create_table:
            self.data_table.create(self.dbconn.con, checkfirst=True)

    def get_primary_schema(self) -> DataSchema:
        return [column for column in self.data_sql_schema if column.primary_key]

    def get_meta_schema(self) -> MetaSchema:
        meta_key_prop = MetaKey.get_property_name()
        return [column for column in self.data_sql_schema if hasattr(column, meta_key_prop)]

    def _apply_where_expression(self, sql, idx: IndexDF):
        if len(self.primary_keys) == 1:
            # Когда ключ один - сравниваем напрямую
            key = self.primary_keys[0]
            sql = sql.where(
                self.data_table.c[key].in_(idx[key].to_list())
            )

        else:
            # Когда ключей много - сравниваем через tuple
            keys = tuple_(*[
                self.data_table.c[key]
                for key in self.primary_keys
            ])

            sql = sql.where(keys.in_([
                tuple([r[key] for key in self.primary_keys])  # type: ignore
                for r in idx.to_dict(orient='records')
            ]))

        return sql

    def delete_rows(self, idx: IndexDF) -> None:
        if idx is not None and len(idx.index):
            logger.debug(f'Deleting {len(idx.index)} rows from {self.name} data')
            sql = self._apply_where_expression(delete(self.data_table), idx)
            self.dbconn.con.execute(sql)

    def insert_rows(self, df: DataDF) -> None:
        if df.empty:
            return

        self.delete_rows(data_to_index(df, self.primary_keys))
        logger.debug(f'Inserting {len(df)} rows into {self.name} data')

        df.to_sql(
            name=self.name,
            con=self.dbconn.con,
            schema=self.dbconn.schema,
            if_exists='append',
            index=False,
            chunksize=1000,
            method='multi',
            dtype=sql_schema_to_sqltype(self.data_sql_schema),
        )

    def update_rows(self, df: DataDF) -> None:
        self.insert_rows(df)

    # Fix numpy types in IndexDF
    def _get_sql_param(self, param):
        return param.item() if hasattr(param, "item") else param

    def read_rows(self, idx: Optional[IndexDF] = None) -> pd.DataFrame:
        sql = select(self.data_table.c)

        if idx is not None:
            if not len(idx.index):
                return pd.DataFrame(columns=[column.name for column in self.data_sql_schema])

            sql = self._apply_where_expression(sql, idx)

        return pd.read_sql_query(
            sql,
            con=self.dbconn.con
        )

    def read_rows_meta_pseudo_df(self, chunksize: int = 1000, run_config: RunConfig = None) -> Iterator[DataDF]:
        sql = select(self.data_table.c)

        sql = sql_apply_runconfig_filter(sql, self.data_table, self.primary_keys, run_config)

        return pd.read_sql_query(
            sql,
            con=self.dbconn.con.execution_options(stream_results=True),
            chunksize=chunksize
        )
