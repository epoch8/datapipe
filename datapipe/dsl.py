from typing import Dict, List

from abc import ABC
from dataclasses import dataclass

from datapipe.datatable import DataStore, DataTable
from datapipe.step import ComputeStep
from datapipe.store.table_store import TableStore


@dataclass
class Table:
    store: TableStore


@dataclass
class ExternalTable(Table):
    '''
    Таблица, которая изменяется снаружи и которую нужно регулярно перечитывать
    (Например таблица с входными картинками)
    '''
    pass


class Catalog:
    def __init__(self, catalog: Dict[str, Table]):
        self.catalog = catalog
        self.data_tables: Dict[str, DataTable] = {}

    def add_table(self, ds: DataStore, name: str, tbl: Table) -> DataTable:
        self.catalog[name] = tbl
        return self.get_datatable(ds, name)

    def get_datatable(self, ds: DataStore, name: str) -> DataTable:
        return ds.get_or_create_table(
            name=name,
            table_store=self.catalog[name].store
        )


class PipelineStep(ABC):
    def build_compute(self, ds: DataStore, catalog: Catalog) -> List[ComputeStep]:
        raise NotImplementedError


@dataclass
class Pipeline:
    steps: List[PipelineStep]
