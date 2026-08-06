from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Generator, Protocol

from datapipe.datatable import DataStore
from datapipe.run_config import RunConfig
from datapipe.types import ChangeList, IndexDF

if TYPE_CHECKING:
    from datapipe.compute import ComputeStep


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
        step: "ComputeStep",
        ds: DataStore,
        idx_count: int,
        idx_gen: Generator[IndexDF, None, None],
        process_fn: ProcessFn,
        run_config: RunConfig | None = None,
        executor_config: ExecutorConfig | None = None,
    ) -> ChangeList: ...


class SingleThreadExecutor(Executor):
    def run_process_batch(
        self,
        step: "ComputeStep",
        ds: DataStore,
        idx_count: int,
        idx_gen: Generator[IndexDF, None, None],
        process_fn: ProcessFn,
        run_config: RunConfig | None = None,
        executor_config: ExecutorConfig | None = None,
    ) -> ChangeList:
        from datapipe.cancel import get_cancel_token

        res_changelist = ChangeList()
        callback = run_config.callback if run_config is not None else None

        if callback is not None:
            callback.on_step_progress(step, 0, idx_count)

        completed = 0
        try:
            for idx in idx_gen:
                token = get_cancel_token()
                if token is not None:
                    token.raise_if_cancelled()
                changes = process_fn(
                    ds=ds,
                    idx=idx,
                    run_config=run_config,
                )
                res_changelist.extend(changes)
                completed += 1
                if callback is not None:
                    callback.on_step_progress(step, completed, idx_count)
        finally:
            idx_gen.close()

        return res_changelist
