from typing import List, TYPE_CHECKING
from dataclasses import dataclass

if TYPE_CHECKING:
    from datapipe.metastore import MetaStore
    from datapipe.datatable import DataTable


@dataclass
class ComputeStep:
    name: str
    input_dts: List['DataTable']
    output_dts: List['DataTable']

    def run(self, ms: 'MetaStore'):
        raise NotImplementedError
