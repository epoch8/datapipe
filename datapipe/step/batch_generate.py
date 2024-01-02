import inspect
import logging
import time
from dataclasses import dataclass
from typing import Callable, Dict, Iterator, List, Optional

import pandas as pd
from opentelemetry import trace

from datapipe.compute import Catalog, ComputeStep, PipelineStep
from datapipe.datatable import DataStore, DataTable
from datapipe.run_config import RunConfig
from datapipe.step.datatable_transform import (
    DatatableTransformFunc,
    DatatableTransformStep,
)
from datapipe.types import Labels, TransformResult, cast

logger = logging.getLogger("datapipe.step.batch_generate")
tracer = trace.get_tracer("datapipe.step.batch_generate")


BatchGenerateFunc = Callable[..., Iterator[TransformResult]]


def do_batch_generate(
    func: BatchGenerateFunc,
    ds: DataStore,
    output_dts: List[DataTable],
    run_config: Optional[RunConfig] = None,
    kwargs: Optional[Dict] = None,
    delete_stale: bool = True,
) -> None:
    """
    Создание новой таблицы из результатов запуска `proc_func`.
    Функция может быть как обычной, так и генерирующейся
    """

    now = time.time()
    empty_generator = True
    parameters = inspect.signature(func).parameters
    kwargs = {
        **({"ds": ds} if "ds" in parameters else {}),
        **(kwargs or {}),
    }

    assert inspect.isgeneratorfunction(
        func
    ), "Starting v0.8.0 proc_func should be a generator"

    with tracer.start_as_current_span("init generator"):
        try:
            iterable = func(**kwargs or {})
        except Exception as e:
            logger.exception(f"Generating failed ({func.__name__}): {str(e)}")
            ds.event_logger.log_exception(e, run_config=run_config)

            raise e

    while True:
        with tracer.start_as_current_span("get next batch"):
            try:
                chunk_dfs = next(iterable)

                if isinstance(chunk_dfs, pd.DataFrame):
                    chunk_dfs = (chunk_dfs,)
            except StopIteration:
                break
            except Exception as e:
                logger.exception(f"Generating failed ({func}): {str(e)}")
                ds.event_logger.log_exception(e, run_config=run_config)

                # raise e
                return

        empty_generator = False

        with tracer.start_as_current_span("store results"):
            for k, dt_k in enumerate(output_dts):
                dt_k.store_chunk(chunk_dfs[k], run_config=run_config)

    if delete_stale:
        with tracer.start_as_current_span("delete stale rows"):
            for k, dt_k in enumerate(output_dts):
                dt_k.delete_stale_by_process_ts(now, run_config=run_config)


@dataclass
class BatchGenerate(PipelineStep):
    func: BatchGenerateFunc
    outputs: List[str]
    kwargs: Optional[Dict] = None
    labels: Optional[Labels] = None
    delete_stale: bool = True

    def build_compute(self, ds: DataStore, catalog: Catalog) -> List[ComputeStep]:
        return [
            DatatableTransformStep(
                name=self.func.__name__,
                func=cast(
                    DatatableTransformFunc,
                    lambda ds, input_dts, output_dts, run_config, kwargs: do_batch_generate(
                        func=self.func,
                        ds=ds,
                        output_dts=output_dts,
                        run_config=run_config,
                        kwargs=kwargs,
                    ),
                ),
                input_dts=[],
                output_dts=[catalog.get_datatable(ds, name) for name in self.outputs],
                check_for_changes=False,
                kwargs=self.kwargs,
                labels=self.labels,
            )
        ]
