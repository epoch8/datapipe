from typing import List, Any, Dict, Union, Optional

import logging
import pandas as pd
from sqlalchemy import Column, Table, String, create_engine, MetaData
from sqlalchemy.sql.expression import select, delete

from datapipe.store.types import Index
from datapipe.store.table_store import TableDataStore


logger = logging.getLogger('datapipe.store.database')


PRIMARY_KEY = [Column('id', String(100), primary_key=True)]


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


class TableStoreDB(TableDataStore):
    def __init__(
        self,
        dbconn: Union['DBConn', str],
        name: str,
        data_sql_schema: List[Column],
        create_table: bool = True
    ) -> None:
        if isinstance(dbconn, str):
            self.dbconn = DBConn(dbconn)
        else:
            self.dbconn = dbconn
        self.name = name

        self.data_sql_schema = PRIMARY_KEY + data_sql_schema

        self.data_table = Table(
            self.name, self.dbconn.sqla_metadata,
            *[i.copy() for i in self.data_sql_schema]
        )

        if create_table:
            self.data_table.create(self.dbconn.con, checkfirst=True)

    def delete_rows(self, idx: Index) -> None:
        if len(idx) > 0:
            logger.debug(f'Deleting {len(idx)} rows from {self.name} data')

            sql = delete(self.data_table).where(self.data_table.c.id.in_(list(idx)))
            self.dbconn.con.execute(sql)

    def insert_rows(self, df: pd.DataFrame) -> None:
        if len(df) > 0:
            logger.debug(f'Inserting {len(df)} rows into {self.name} data')

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

    def update_rows(self, df: pd.DataFrame) -> None:
        self.delete_rows(df.index)
        self.insert_rows(df)

    def read_rows(self, idx: Optional[Index] = None) -> pd.DataFrame:
        if idx is None:
            return pd.read_sql_query(
                select([self.data_table]),
                con=self.dbconn.con,
                index_col='id',
            )
        else:
            return pd.read_sql_query(
                select([self.data_table]).where(self.data_table.c.id.in_(list(idx))),
                con=self.dbconn.con,
                index_col='id',
            )
