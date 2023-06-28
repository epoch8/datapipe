from typing import Any, Dict, Iterable, Optional

import ray
from tqdm_loggable.auto import tqdm

from datapipe.datatable import DataStore
from datapipe.executor import Executor, ExecutorConfig, ProcessFn
from datapipe.run_config import RunConfig
from datapipe.types import ChangeList, IndexDF


class RayExecutor(Executor):
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

        remote_kwargs: Dict[str, Any] = {}

        if executor_config is not None:
            if executor_config.memory is not None:
                remote_kwargs["memory"] = executor_config.memory
            if executor_config.cpu is not None:
                remote_kwargs["num_cpus"] = executor_config.cpu

        if remote_kwargs:

            @ray.remote(**remote_kwargs)
            def process_fn_remote(ds, idx, run_config):
                return process_fn(ds, idx, run_config)

        else:

            @ray.remote
            def process_fn_remote(ds, idx, run_config):
                return process_fn(ds, idx, run_config)

        # Submit tasks to remote functions using Ray
        futures = []
        for idx in idx_gen:
            future = process_fn_remote.remote(ds, idx, run_config)
            futures.append(future)

        # Generator to collect results, so tqdm can show progress
        def _results(futures):
            ready, futures = ray.wait(futures, timeout=None)
            while len(ready) > 0:
                for result in ray.get(ready):
                    yield result
                ready, futures = ray.wait(futures, timeout=None)

        for result in tqdm(_results(futures), total=len(futures)):
            res_changelist.extend(result)

        return res_changelist
