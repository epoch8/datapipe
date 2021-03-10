import anyconfig
from dataclasses import dataclass
from typing import Dict, List, Tuple
import sqlalchemy as sql
from sqlalchemy import create_engine
from sqlalchemy.sql.sqltypes import (
    String, Integer, Float, JSON
)

from c12n_pipe.datatable import DataStore, DataTable


COLUMN_TYPE_TO_SQL_COLUMN = {
    'string': String,
    'str': String,
    'int': Integer,
    'float': Float,
    'json': JSON
}


@dataclass
class AbstractDataTable:
    def __init__(
        self,
        data_sql_schema: List[Tuple[str, sql.Column]]
    ):
        self.data_sql_schema = data_sql_schema


class DataCatalog:
    def __init__(
        self,
        ds: DataStore,
        catalog: Dict[str, AbstractDataTable],
    ):
        self.ds = ds
        self.catalog = catalog

        self.catalog_tables = {
            name: self.ds.get_table(name, t.data_sql_schema, create_tables=True)
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
        ds: DataStore,
    ):
        catalog = {
            k: v
            for k, v in anyconfig.load(config_path).items()
            if not k.startswith("_")
        }
        catalog = {
            name: AbstractDataTable(
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
