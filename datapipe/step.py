from typing import List, TYPE_CHECKING, Dict, Any
from dataclasses import dataclass

if TYPE_CHECKING:
    from datapipe.datatable import DataStore, DataTable


@dataclass
class RunConfig:
    # Массив глобально применяемых фильтров
    # если не пуст, то во время запуска обрабатываются только те строки,
    # которые строго соответствуют фильтру
    # (в случае, если у таблицы есть идентификатор с совпадающим именем).
    filters: Dict[str, Any]
     
    labels: Dict[str, Any]


@dataclass
class ComputeStep:
    name: str
    input_dts: List['DataTable']
    output_dts: List['DataTable']

    def run(self, ds: 'DataStore', run_config: RunConfig = None) -> None:
        raise NotImplementedError
