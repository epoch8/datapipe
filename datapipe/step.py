from typing import List, TYPE_CHECKING, Dict, Any, Optional
from dataclasses import dataclass

if TYPE_CHECKING:
    from datapipe.datatable import DataStore, DataTable


LabelDict = Dict[str, Any]


@dataclass
class RunConfig:
    # Массив глобально применяемых фильтров
    # если не пуст, то во время запуска обрабатываются только те строки,
    # которые строго соответствуют фильтру
    # (в случае, если у таблицы есть идентификатор с совпадающим именем).
    filters: LabelDict
    labels: LabelDict

    def __init__(self, filters: LabelDict = None, labels: LabelDict = None) -> None:
        self.filters = filters if filters else {}
        self.labels = labels if labels else {}

    @classmethod
    def add_labels(cls, rc: Optional['RunConfig'], labels: LabelDict) -> 'RunConfig':
        if rc is not None:
            return RunConfig(
                filters=rc.filters,
                labels={**rc.labels, **labels},
            )
        else:
            return RunConfig(
                labels=labels,
            )


@dataclass
class ComputeStep:
    name: str
    input_dts: List['DataTable']
    output_dts: List['DataTable']

    def run(self, ds: 'DataStore', run_config: RunConfig = None) -> None:
        raise NotImplementedError
