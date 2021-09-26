from typing import List, TYPE_CHECKING
from dataclasses import dataclass

if TYPE_CHECKING:
    from datapipe.datatable import DataStore, DataTable


@dataclass
class ComputeStep:
    name: str
    input_dts: List['DataTable']
    output_dts: List['DataTable']

    def run(self, ds: 'DataStore'):
        raise NotImplementedError
