from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Iterable, Optional, Protocol

from tqdm_loggable.auto import tqdm

from datapipe.datatable import DataStore
from datapipe.run_config import RunConfig
from datapipe.types import ChangeList, IndexDF


class ProcessFn(Protocol):
    def __call__(
        self,
        ds: DataStore,
        idx: IndexDF,
        run_config: Optional[RunConfig] = None,
    ) -> ChangeList: ...


@dataclass
class ExecutorConfig:
    memory: Optional[int] = None
    cpu: Optional[float] = None
    gpu: Optional[int] = None

    parallelism: int = 100


class Executor(ABC):
    @abstractmethod
    def run_process_batch(
        self,
        name: str,
        ds: DataStore,
        idx_count: int,
        idx_gen: Iterable[IndexDF],
        process_fn: ProcessFn,
        run_config: Optional[RunConfig] = None,
        executor_config: Optional[ExecutorConfig] = None,
    ) -> ChangeList: ...


class SingleThreadExecutor(Executor):
    def run_process_batch(
        self,
        name: str,
        ds: DataStore,
        idx_count: int,
        idx_gen: Iterable[IndexDF],
        process_fn: ProcessFn,
        run_config: Optional[RunConfig] = None,
        executor_config: Optional[ExecutorConfig] = None,
    ) -> ChangeList:
        res_changelist = ChangeList()

        for idx in tqdm(idx_gen, total=idx_count):
            changes = process_fn(
                ds=ds,
                idx=idx,
                run_config=run_config,
            )

            res_changelist.extend(changes)

        return res_changelist
