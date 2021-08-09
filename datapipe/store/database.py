from typing import List, Any, Dict, Union, Optional

import logging
import pandas as pd

from dataclasses import dataclass
from sqlalchemy import Column, Table, Integer, create_engine, MetaData
from sqlalchemy.sql.expression import select, delete

from datapipe.store.types import Index
from datapipe.store.table_store import TableStore


logger = logging.getLogger('datapipe.store.database')


PRIMARY_KEY = [Column('id', Integer(), primary_key=True)]


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

        const_idx_schema = []

        if const_idx:
            for item in const_idx:
                item.column.primary_key = True
                self.filters[item.column.name] = item.value

                const_idx_schema.append(item.column)

        self.data_sql_schema = PRIMARY_KEY + const_idx_schema + data_sql_schema

        self.data_table = Table(
            self.name, self.dbconn.sqla_metadata,
            *[i.copy() for i in self.data_sql_schema],
            extend_existing=True
        )

        if create_table:
            self.data_table.create(self.dbconn.con, checkfirst=True)

    def get_meta_schema(self, meta_keys: List[str]) -> List[Column]:
        columns = [column for column in self.data_sql_schema if column.name in meta_keys]
        find_keys = [column.name for column in columns]

        if sorted(meta_keys) != sorted(find_keys):
            raise ValueError('Finded columns in TableStoreDB doesn`t equal meta_keys')

        return columns

    def delete_rows(self, idx: Index) -> None:
        if len(idx) > 0:
            logger.debug(f'Deleting {len(idx)} rows from {self.name} data')

            sql = delete(self.data_table).where(
                self.data_table.c.id.in_(list(idx))
            )

            if self.filters:
                for key in self.filters.keys():
                    column = getattr(self.data_table.c, key)
                    sql = sql.where(column == self.filters[key])

            self.dbconn.con.execute(sql)

    def insert_rows(self, df: pd.DataFrame) -> None:
        if len(df) > 0:
            logger.debug(f'Inserting {len(df)} rows into {self.name} data')

            if self.filters:
                if set(self.filters.keys()) & set(df.columns):
                    raise ValueError("DataFrame has constant index columns")

                for key in self.filters.keys():
                    df[key] = self.filters[key]

            df.to_sql(
                name=self.name,
                con=self.dbconn.con,
                schema=self.dbconn.schema,
                if_exists='append',
                index_label='id',
                chunksize=1000,
                method='multi',
                dtype=sql_schema_to_dtype(self.data_sql_schema),
            )

            if self.filters:
                for key in self.filters.keys():
                    del df[key]

    def update_rows(self, df: pd.DataFrame) -> None:
        self.delete_rows(df.index)
        self.insert_rows(df)

    def read_rows(self, idx: Optional[Index] = None) -> pd.DataFrame:
        exclude_cols = self.filters.keys() if self.filters else []
        sql_fields = [col for col in self.data_table.columns
                      if col.name not in exclude_cols]
        sql = select(sql_fields)

        if idx is not None:
            sql = sql.where(self.data_table.c.id.in_(list(idx)))

        if self.filters:
            for key in self.filters.keys():
                column = getattr(self.data_table.c, key)
                sql = sql.where(column == self.filters[key])

        return pd.read_sql_query(
            sql,
            con=self.dbconn.con,
            index_col='id',
        )
