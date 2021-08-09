from typing import Callable, Dict, List, Tuple, Union

from abc import ABC
from dataclasses import dataclass

from datapipe.datatable import DataTable
from datapipe.store.table_store import TableStore
from datapipe.metastore import MetaStore


@dataclass
class Table:
    meta_keys: List[str]
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

    def get_datatable(self, ms: MetaStore, name: str) -> DataTable:
        if name not in self.data_tables:
            meta_keys = self.catalog[name].meta_keys

            if 'id' in meta_keys:
                raise ValueError('meta_keys cannot contain a item with a name `id`')

            store = self.catalog[name].store
            meta_schema = store.get_meta_schema(meta_keys)

            self.data_tables[name] = DataTable(
                name=name,
                meta_keys=meta_keys,
                meta_table=ms.create_meta_table(name, meta_schema),
                table_store=store

            )

        return self.data_tables[name]


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
