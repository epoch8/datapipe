from typing import Iterable, Optional

import ray
from tqdm_loggable.auto import tqdm

from datapipe.datatable import DataStore
from datapipe.executor import Executor, ProcessFn
from datapipe.run_config import RunConfig
from datapipe.types import ChangeList, IndexDF


# Define a remote function for the process_fn
@ray.remote
def process_fn_remote(process_fn, ds, idx, run_config):
    return process_fn(ds, idx, run_config)


class RayExecutor(Executor):
    def run_process_batch(
        self,
        ds: DataStore,
        idx_count: int,
        idx_gen: Iterable[IndexDF],
        process_fn: ProcessFn,
        run_config: Optional[RunConfig] = None,
    ) -> ChangeList:
        res_changelist = ChangeList()

        # Submit tasks to remote functions using Ray
        futures = []
        for idx in idx_gen:
            future = process_fn_remote.remote(process_fn, ds, idx, run_config)
            futures.append(future)

        # Generator to collect results, so tqdm can show progress
        def _results(futures):
            ready, futures = ray.wait(futures, timeout=None)
            while len(ready) > 0:
                for result in ray.get(ready):
                    yield result
                ready, remaining = ray.wait(futures, timeout=None)

        for result in tqdm(_results(futures), total=len(futures)):
            res_changelist.extend(result)

        return res_changelist
