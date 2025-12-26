from abc import ABC, abstractmethod
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Protocol

from tqdm_loggable.auto import tqdm

from datapipe.datatable import DataStore
from datapipe.run_config import RunConfig
from datapipe.types import ChangeList, IndexDF


class ProcessFn(Protocol):
    def __call__(
        self,
        ds: DataStore,
        idx: IndexDF,
        run_config: RunConfig | None = None,
    ) -> ChangeList: ...


@dataclass
class ExecutorConfig:
    memory: int | None = None
    cpu: float | None = None
    gpu: int | None = None

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
        run_config: RunConfig | None = None,
        executor_config: ExecutorConfig | None = None,
    ) -> ChangeList: ...


class SingleThreadExecutor(Executor):
    def run_process_batch(
        self,
        name: str,
        ds: DataStore,
        idx_count: int,
        idx_gen: Iterable[IndexDF],
        process_fn: ProcessFn,
        run_config: RunConfig | None = None,
        executor_config: ExecutorConfig | None = None,
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
