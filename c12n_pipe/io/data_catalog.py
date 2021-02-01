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

    def get_data_table(
        self,
        name: str,
        data_store: DataStore
    ) -> DataTable:
        return data_store.get_table(
            name=name,
            data_sql_schema=self.data_sql_schema
        )


class DataCatalog:
    def __init__(
        self,
        catalog: Dict[str, AbstractDataTable],
        connstr: str,
        schema: str
    ):
        self.catalog = catalog
        self.connstr = connstr
        self.schema = schema

        eng = create_engine(connstr)
        if not eng.dialect.has_schema(eng, schema=schema):
            eng.execute(f'CREATE SCHEMA {schema};')

        self.data_store = DataStore(
            connstr=connstr,
            schema=schema
        )

    def get_data_table(self, name: str):
        return self.catalog[name].get_data_table(name, self.data_store)

    @classmethod
    def from_config(
        cls,
        config_path: str,
        connstr: str,
        schema: str
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
            catalog=catalog,
            connstr=connstr,
            schema=schema
        )
