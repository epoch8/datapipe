from collections.abc import Generator
from dataclasses import replace
from typing import TYPE_CHECKING, Any

import ray

from datapipe.datatable import DataStore
from datapipe.executor import Executor, ExecutorConfig, ProcessFn
from datapipe.run_config import RunConfig
from datapipe.types import ChangeList, IndexDF

if TYPE_CHECKING:
    from datapipe.compute import ComputeStep


class RayExecutor(Executor):
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
        res_changelist = ChangeList()
        callback = run_config.callback if run_config is not None else None

        remote_kwargs: dict[str, Any] = {
            "name": step.name,
        }

        if executor_config is not None:
            if executor_config.memory is not None:
                remote_kwargs["memory"] = executor_config.memory
            if executor_config.cpu is not None:
                remote_kwargs["num_cpus"] = executor_config.cpu
            if executor_config.gpu is not None:
                remote_kwargs["num_gpus"] = executor_config.gpu

            parallelism = executor_config.parallelism
        else:
            parallelism = 10

        @ray.remote(**remote_kwargs)
        def process_fn_remote(ds, idx, run_config):
            return process_fn(ds, idx, run_config)

        # Progress reporting happens driver-side per completed future; the remote
        # worker never invokes callbacks, so avoid shipping them over the wire.
        remote_run_config = replace(run_config, callback=None) if run_config is not None else None

        # Generator to collect results as Ray futures resolve
        def _results(idx_gen: Generator[IndexDF, None, None]) -> Generator[ChangeList, None, None]:
            from datapipe.cancel import get_cancel_token

            # Submit tasks to remote functions using Ray
            futures: list[ray.ObjectRef[ChangeList]] = []
            try:
                for idx in idx_gen:
                    token = get_cancel_token()
                    if token is not None and token.is_cancelled():
                        for future in futures:
                            ray.cancel(future, force=True)
                        token.raise_if_cancelled()

                    if len(futures) > parallelism:
                        ready, futures = ray.wait(futures, timeout=None)
                        for result in ray.get(ready):
                            yield result

                    future = process_fn_remote.remote(ds, idx, remote_run_config)
                    futures.append(future)

                ready, futures = ray.wait(futures, timeout=None)
                while len(ready) > 0:
                    token = get_cancel_token()
                    if token is not None and token.is_cancelled():
                        for future in futures:
                            ray.cancel(future, force=True)
                        token.raise_if_cancelled()
                    for result in ray.get(ready):
                        yield result
                    ready, futures = ray.wait(futures, timeout=None)
            except BaseException:
                for future in futures:
                    ray.cancel(future, force=True)
                raise

        if callback is not None:
            callback.on_step_progress(step, 0, idx_count)

        completed = 0
        try:
            for result in _results(idx_gen):
                res_changelist.extend(result)
                completed += 1
                if callback is not None:
                    callback.on_step_progress(step, completed, idx_count)
        finally:
            idx_gen.close()

        return res_changelist
