from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Iterable, Optional, Protocol

from datapipe.datatable import DataStore
from datapipe.run_config import RunConfig
from datapipe.types import ChangeList, IndexDF
from tqdm_loggable.auto import tqdm


class ProcessFn(Protocol):
    def __call__(
        self,
        ds: DataStore,
        idx: IndexDF,
        run_config: Optional[RunConfig] = None,
    ) -> ChangeList:
        ...


class Executor(ABC):
    @abstractmethod
    def run_process_batch(
        self,
        ds: DataStore,
        idx_count: int,
        idx_gen: Iterable[IndexDF],
        process_fn: ProcessFn,
        run_config: Optional[RunConfig] = None,
    ) -> ChangeList:
        ...


class SingleThreadExecutor(Executor):
    def run_process_batch(
        self,
        ds: DataStore,
        idx_count: int,
        idx_gen: Iterable[IndexDF],
        process_fn: ProcessFn,
        run_config: Optional[RunConfig] = None,
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


class MultiThreadExecutor(Executor):
    def __init__(self, workers: int = 4):
        self._executor = ThreadPoolExecutor(max_workers=workers)

    def run_process_batch(
        self,
        ds: DataStore,
        idx_count: int,
        idx_gen: Iterable[IndexDF],
        process_fn: ProcessFn,
        run_config: Optional[RunConfig] = None,
    ) -> ChangeList:
        res_changelist = ChangeList()

        futures = []
        for idx in idx_gen:
            future = self._executor.submit(
                process_fn,
                ds=ds,
                idx=idx,
                run_config=run_config,
            )
            futures.append(future)

        for future in tqdm(as_completed(futures), total=idx_count):
            changes = future.result()
            res_changelist.extend(changes)

        return res_changelist
