from typing import Callable, Dict, List, Tuple

from abc import ABC
from dataclasses import dataclass

from datapipe.datatable import DataTable
from datapipe.store.table_store import TableStore
from datapipe.metastore import MetaStore


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


@dataclass
class Catalog:
    catalog: Dict[str, Table]

    def get_datatable(self, ms: MetaStore, name: str) -> DataTable:
        return DataTable(ms, name, self.catalog[name].store)


class PipelineStep(ABC):
    pass


@dataclass
class Pipeline:
    steps: List[PipelineStep]


@dataclass
class BatchTransform(PipelineStep):
    func: Callable
    inputs: List[str]
    outputs: List[str]
    chunk_size: int = 1000


@dataclass
class BatchGenerate(PipelineStep):
    func: Callable
    outputs: List[str]


@dataclass
class LabelStudioModeration:
    ls_url: str
    project_setting: Dict[str, str]
    inputs: List[str]
    outputs: List[str]
    auth: Tuple[str, str]
    chunk_size: int = 1000
