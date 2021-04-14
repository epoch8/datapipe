from dataclasses import dataclass
from typing import List, Callable

from c12n_pipe.metastore import MetaStore
from c12n_pipe.datatable import DataTable, inc_process_many

@dataclass
class ComputeStep:
    name: str
    input_dts: List[DataTable]
    output_dts: List[DataTable]

    def run(self, ms: MetaStore):
        raise NotImplementedError


@dataclass
class IncStep(ComputeStep):
    func: Callable
    chunk_size: int

    def run(self, ms: MetaStore):
        return inc_process_many(
            ms,
            self.input_dts,
            self.output_dts,
            self.func,
            self.chunk_size
        )