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
        name: str,
        data_sql_schema: List[Tuple[str, sql.Column]]
    ):
        self.name = name
        self.data_sql_schema = data_sql_schema

    def get_data_table(
        self,
        data_store: DataStore
    ) -> DataTable:
        return data_store.get_table(
            name=self.name,
            data_sql_schema=self.data_sql_schema
        )


@dataclass
class DataTableConfig:
    type: str
    columns: Dict[str, str]


class DataCatalog:
    def __init__(
        self,
        catalog: DataTableConfig,
        connstr: str,
        schema: str
    ):
        self.catalog = catalog
        self.connstr = connstr
        self.schema = schema

        eng = create_engine(connstr)
        if not eng.dialect.has_schema(eng, schema=schema):
            eng.execute(f'CREATE SCHEMA {schema};')

        self.name_to_abstract_data_tables = {
            name: AbstractDataTable(
                name=name,
                data_sql_schema=[
                    sql.Column(column, COLUMN_TYPE_TO_SQL_COLUMN[column_type])
                    for column, column_type in config['columns'].items()
                    if config['type'] == 'DataTable'
                ]
            )
            for name, config in self.catalog.items()
        }

        self.data_store = DataStore(
            connstr=connstr,
            schema=schema
        )

    def get_data_table(self, name: str):
        return self.name_to_abstract_data_tables[name].get_data_table(self.data_store)

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
        return cls(
            catalog=catalog,
            connstr=connstr,
            schema=schema
        )
