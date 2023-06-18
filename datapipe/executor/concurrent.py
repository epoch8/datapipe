import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from typing import Iterable, Optional

from tqdm_loggable.auto import tqdm

from datapipe.datatable import DataStore
from datapipe.executor import Executor, ExecutorConfig, ProcessFn
from datapipe.run_config import RunConfig
from datapipe.types import ChangeList, IndexDF


class MultiProcessExecutor(Executor):
    def __init__(self, workers: int = 4):
        self._executor = ProcessPoolExecutor(
            max_workers=workers,
            mp_context=mp.get_context("spawn"),
        )

    def run_process_batch(
        self,
        ds: DataStore,
        idx_count: int,
        idx_gen: Iterable[IndexDF],
        process_fn: ProcessFn,
        run_config: Optional[RunConfig] = None,
        executor_config: Optional[ExecutorConfig] = None,
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
        executor_config: Optional[ExecutorConfig] = None,
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
