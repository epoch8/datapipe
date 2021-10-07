from typing import List, TYPE_CHECKING
from dataclasses import dataclass

if TYPE_CHECKING:
    from datapipe.datatable import DataStore, DataTable


@dataclass
class ComputeStep:
    name: str
    input_dts: List['DataTable']
    output_dts: List['DataTable']

    def __post_init__(self):
        self.input_dts_primary_keys: List[str] = sorted(
            set.intersection(
                *[set(input_dt.primary_keys) for input_dt in self.input_dts]
            )
        ) if len(self.input_dts) > 0 else []

    def run(self, ds: 'DataStore'):
        raise NotImplementedError
