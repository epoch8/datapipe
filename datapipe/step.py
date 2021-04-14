from typing import List, TYPE_CHECKING
from dataclasses import dataclass

if TYPE_CHECKING:
    from c12n_pipe.metastore import MetaStore
    from c12n_pipe.datatable import DataTable


@dataclass
class ComputeStep:
    name: str
    input_dts: List['DataTable']
    output_dts: List['DataTable']

    def run(self, ms: 'MetaStore'):
        raise NotImplementedError

