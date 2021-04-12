import anyconfig
from dataclasses import dataclass
from typing import Dict, List, Tuple
import sqlalchemy as sql
from sqlalchemy.sql.sqltypes import (
    String, Integer, Float, JSON
)

from c12n_pipe.metastore import MetaStore, PRIMARY_KEY
from c12n_pipe.datatable import DataTable
from c12n_pipe.store.table_store_sql import TableStoreDB


COLUMN_TYPE_TO_SQL_COLUMN = {
    'string': String,
    'str': String,
    'int': Integer,
    'float': Float,
    'json': JSON
}


@dataclass
class DBTable:
    def __init__(
        self,
        data_sql_schema: List[Tuple[str, sql.Column]]
    ):
        self.data_sql_schema = data_sql_schema

# class JSONFiledir:

# class PILImageFiledir:
#     ...


class DataCatalog:
    def __init__(
        self,
        ds: MetaStore,
        catalog: Dict[str, DBTable],
    ):
        self.ds = ds
        self.catalog = catalog

        self.catalog_tables = {
            name: DataTable(self.ds, name, data_store=TableStoreDB(self.ds.dbconn, f'{name}_data', PRIMARY_KEY + t.data_sql_schema, True))
            for name, t in self.catalog.items()
        }

        # eng = create_engine(connstr)
        # if not eng.dialect.has_schema(eng, schema=schema):
        #     eng.execute(f'CREATE SCHEMA {schema};')

    def get_data_table(self, name: str):
        return self.catalog_tables[name]

    @classmethod
    def from_config(
        cls,
        config_path: str,
        ds: MetaStore,
    ):
        catalog = {
            k: v
            for k, v in anyconfig.load(config_path).items()
            if not k.startswith("_")
        }
        catalog = {
            name: DBTable(
                data_sql_schema=[
                    sql.Column(column, COLUMN_TYPE_TO_SQL_COLUMN[column_type])
                    for column, column_type in config['columns'].items()
                    if config['type'] == 'DataTable'
                ]
            )
            for name, config in catalog.items()
        }
        return cls(
            ds=ds,
            catalog=catalog,
        )
