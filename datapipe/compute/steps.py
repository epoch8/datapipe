from dataclasses import dataclass
from typing import List, Callable

from .metastore import MetaStore


@dataclass
class ComputeStep:
    name: str
    inputs: List[str]
    outputs: List[str]

    def run(self, ms: MetaStore):
        raise NotImplementedError


@dataclass
class IncStep(ComputeStep):
    func: Callable

    def run(self, ms: MetaStore):
        pass