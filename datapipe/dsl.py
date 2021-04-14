from c12n_pipe.datatable import DataTable
from typing import Callable, Dict, List, IO, Any, Union

from abc import ABC
from dataclasses import dataclass
from pathlib import Path

from c12n_pipe.store.table_store_filedir import TableStoreFiledir
from c12n_pipe.store.table_store import TableDataStore
from c12n_pipe.metastore import MetaStore


@dataclass
class Table:
    store: TableDataStore


@dataclass
class ExternalTable(Table):
    '''
    Таблица, которая изменяется снаружи и которую нужно регулярно перечитывать
    (Например таблица с входными картинками)
    '''
    pass


@dataclass
class Catalog:
    catalog: Dict[str, Table]

    def get_datatable(self, ms: MetaStore, name: str):
        return DataTable(ms, name, self.catalog[name].store)


class PipelineStep(ABC):
    pass


@dataclass
class Pipeline:
    steps: List[PipelineStep]


@dataclass
class Transform(PipelineStep):
    func: Callable
    inputs: List[str]
    outputs: List[str]
    chunk_size: int = 1000
