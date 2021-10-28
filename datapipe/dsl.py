from typing import Callable, Dict, List, Tuple, Union

from abc import ABC
from dataclasses import dataclass

from datapipe.datatable import DataStore, DataTable
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

    def get_datatable(self, ds: DataStore, name: str) -> DataTable:
        return ds.get_or_create_table(
            name=name,
            table_store=self.catalog[name].store
        )


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
class LabelStudioModeration(PipelineStep):
    ls_url: str
    inputs: List[str]
    outputs: List[str]
    auth: Tuple[str, str]
    project_title: str
    project_description: str
    project_label_config: str
    data: List[str]
    annotations: Union[str, None] = None
    predictions: Union[str, None] = None
    chunk_size: int = 100
