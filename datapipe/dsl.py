from typing import Callable, Dict, List, IO, Any, Union

from abc import ABC
from dataclasses import dataclass
from pathlib import Path


class TableStore(ABC):
    pass


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


class FileStoreAdapter(ABC):
    def load(self, f: IO) -> Dict[str, Any]:
        raise NotImplemented

    def dump(self, obj: Dict[str, Any], f: IO) -> None:
        raise NotImplemented


class Filedir(TableStore):
    def __init__(
        self, 
        filename_pattern: Union[str, Path],
        adapter: FileStoreAdapter
    ):    
        if isinstance(filename_pattern, Path):
            self.filename_pattern = str(filename_pattern.resolve())
        else:
            self.filename_pattern = filename_pattern
        
        self.adapter = adapter
