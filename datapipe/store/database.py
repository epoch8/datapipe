from typing import List, Any, Dict, Union, Optional

import logging
import pandas as pd

from dataclasses import dataclass
from sqlalchemy import Column, Table, Integer, create_engine, MetaData
from sqlalchemy.sql.expression import select, delete, and_, or_
from datapipe.store.types import Index, DataSchema
from datapipe.store.table_store import TableStore


logger = logging.getLogger('datapipe.store.database')


def sql_schema_to_dtype(schema: List[Column]) -> Dict[str, Any]:
    return {
        i.name: i.type for i in schema
    }


class DBConn:
    def __init__(self, connstr: str, schema: str = None):
        self._init(connstr, schema)

    def _init(self, connstr: str, schema: Optional[str]) -> None:
        self.connstr = connstr
        self.schema = schema

        self.con = create_engine(
            connstr,
        )

        self.sqla_metadata = MetaData(schema=schema)

    def __getstate__(self):
        return {
            'connstr': self.connstr,
            'schema': self.schema
        }

    def __setstate__(self, state):
        self._init(state['connstr'], state['schema'])


@dataclass
class ConstIdx:
    column: Column
    value: Any


class TableStoreDB(TableStore):
    def __init__(
        self,
        dbconn: Union['DBConn', str],
        name: str,
        data_sql_schema: List[Column],
        create_table: bool = True,
        const_idx: List[ConstIdx] = []
    ) -> None:
        if isinstance(dbconn, str):
            self.dbconn = DBConn(dbconn)
        else:
            self.dbconn = dbconn
        self.name = name
        self.filters = {}

        # TODO remove when filters are implemented
        """
        const_idx_schema = []

        if const_idx:
            for item in const_idx:
                item.column.primary_key = True
                self.filters[item.column.name] = item.value

                const_idx_schema.append(item.column)

        self.data_sql_schema = const_idx_schema + data_sql_schema
        """

        self.data_sql_schema = data_sql_schema
        self.primary_keys = [column.name for column in self.data_sql_schema if column.primary_key]

        self.data_table = Table(
            self.name, self.dbconn.sqla_metadata,
            *[i.copy() for i in self.data_sql_schema],
            extend_existing=True
        )

        if create_table:
            self.data_table.create(self.dbconn.con, checkfirst=True)

    def get_primary_schema(self) -> DataSchema:
        return [column for column in self.data_sql_schema if column.primary_key]

    def delete_rows(self, idx: Index) -> None:
        if len(idx.index):
            logger.debug(f'Deleting {len(idx.index)} rows from {self.name} data')

            row_queries = []

            for _, row in idx.iterrows():
                and_params = [self.data_table.c[key] == self._get_sql_param(row[key]) for key in self.primary_keys]
                and_query = and_(*and_params)
                row_queries.append(and_query)

            sql = delete(self.data_table).where(or_(*row_queries))

            """
            if self.filters:
                for key in self.filters.keys():
                    column = getattr(self.data_table.c, key)
                    sql = sql.where(column == self.filters[key])
            """
            self.dbconn.con.execute(sql)

    def insert_rows(self, df: pd.DataFrame) -> None:
        if len(df) > 0:
            logger.debug(f'Inserting {len(df)} rows into {self.name} data')
            """
            if self.filters:
                if set(self.filters.keys()) & set(df.columns):
                    raise ValueError("DataFrame has constant index columns")

                for key in self.filters.keys():
                    df[key] = self.filters[key]
            """

            df.to_sql(
                name=self.name,
                con=self.dbconn.con,
                schema=self.dbconn.schema,
                if_exists='append',
                index=False,
                chunksize=1000,
                method='multi',
                dtype=sql_schema_to_dtype(self.data_sql_schema),
            )
            """
            if self.filters:
                for key in self.filters.keys():
                    del df[key]
            """

    def update_rows(self, df: pd.DataFrame) -> None:
        self.delete_rows(df)
        self.insert_rows(df)

    # Fix numpy types in Index
    def _get_sql_param(self, param):
        return param.item() if hasattr(param, "item") else param

    def read_rows(self, idx: Optional[Index] = None) -> pd.DataFrame:
        sql = select(self.data_table.c)

        if idx is not None:
            row_queries = []

            for _, row in idx.iterrows():
                and_params = [self.data_table.c[key] == self._get_sql_param(row[key]) for key in self.primary_keys]
                and_query = and_(*and_params)
                row_queries.append(and_query)

            if not row_queries:
                return pd.DataFrame(columns=[column.name for column in self.data_sql_schema])

            sql = sql.where(or_(*row_queries))

        """
        if self.filters:
            for key in self.filters.keys():
                column = getattr(self.data_table.c, key)
                sql = sql.where(column == self.filters[key])
        """
        return pd.read_sql_query(
            sql,
            con=self.dbconn.con
        )
