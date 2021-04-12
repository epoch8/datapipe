from typing import List, Any, Dict, Optional, TYPE_CHECKING

import logging
import pandas as pd
from sqlalchemy import Column, Table, String
from sqlalchemy.sql.expression import select, delete

from c12n_pipe.store.types import Index
from c12n_pipe.store.table_store import TableStore

if TYPE_CHECKING:
    from c12n_pipe.metastore import DBConn


logger = logging.getLogger('c12n_pipe.store.table_store_sql')


def sql_schema_to_dtype(schema: List[Column]) -> Dict[str, Any]:
    return {
        i.name: i.type for i in schema
    }


class TableStoreDB(TableStore):
    def __init__(self, 
        dbconn: 'DBConn',
        name: str,
        data_sql_schema: List[Column],
        create_table: bool = True
    ) -> None:
        self.dbconn = dbconn
        self.name = name

        self.data_sql_schema = data_sql_schema

        self.data_table = Table(
            self.name, self.dbconn.sqla_metadata,
            *[i.copy() for i in self.data_sql_schema]
        )

        if create_table:
            self.data_table.create(self.dbconn.con, checkfirst=True)

    def delete_rows(self, idx: Index) -> None:
        if len(idx) > 0:
            logger.info(f'Deleting {len(idx)} rows from {self.name} data')

            sql = delete(self.data_table).where(self.data_table.c.id.in_(list(idx)))
            self.dbconn.con.execute(sql)

    def insert_rows(self, df: pd.DataFrame) -> None:
        if len(df) > 0:
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

