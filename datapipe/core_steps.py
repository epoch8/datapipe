from typing import Any, Callable, Dict, List, Iterator, Optional, Tuple, Union, cast, Protocol
from dataclasses import dataclass, field

import time
import tqdm
import logging
from opentelemetry import trace

from datapipe.types import DataDF
from datapipe.compute import PipelineStep, Catalog, DatatableTransformStep
from datapipe.datatable import DataStore, DataTable
from datapipe.run_config import RunConfig


logger = logging.getLogger('datapipe.core_steps')
tracer = trace.get_tracer("datapipe.core_steps")


class BatchTransformFunc(Protocol):
    # __name__: str
    def __call__(self, *inputs, **kwargs) -> Union[DataDF, List[DataDF], Tuple[DataDF, ...]]: ...


def batch_transform_wrapper(
    func: BatchTransformFunc,
    ds: DataStore,
    input_dts: List[DataTable],
    output_dts: List[DataTable],
    chunksize: int = 1000,
    run_config: RunConfig = None,
    **kwargs
) -> None:
    import math

    '''
    Множественная инкрементальная обработка `input_dts' на основе изменяющихся индексов
    '''

    with tracer.start_as_current_span("compute ids to process"):
        idx_count, idx_gen = ds.get_process_ids(
            inputs=input_dts,
            outputs=output_dts,
            chunksize=chunksize,
            run_config=run_config
        )

    logger.info(f'Items to update {idx_count}')

    if idx_count > 0:
        for idx in tqdm.tqdm(idx_gen, total=math.ceil(idx_count / chunksize)):
            with tracer.start_as_current_span("process batch"):
                logger.debug(f'Idx to process: {idx.to_records()}')

                with tracer.start_as_current_span("get input data"):
                    input_dfs = [inp.get_data(idx) for inp in input_dts]

                if sum(len(j) for j in input_dfs) > 0:
                    with tracer.start_as_current_span("run transform"):
                        try:
                            chunks_df = func(*input_dfs, **kwargs)
                        except Exception as e:
                            logger.error(f"Transform failed ({func.__name__}): {str(e)}")  # type: ignore # mypy bug: https://github.com/python/mypy/issues/10976
                            ds.event_logger.log_exception(e, run_config=run_config)

                            continue

                    if isinstance(chunks_df, (list, tuple)):
                        assert len(chunks_df) == len(output_dts)
                    else:
                        assert len(output_dts) == 1
                        chunks_df = [chunks_df]

                    with tracer.start_as_current_span("store output batch"):
                        for k, res_dt in enumerate(output_dts):
                            # Берем k-ое значение функции для k-ой таблички
                            # Добавляем результат в результирующие чанки
                            res_dt.store_chunk(
                                data_df=chunks_df[k],
                                processed_idx=idx,
                                run_config=run_config,
                            )

                else:
                    with tracer.start_as_current_span("delete missing data from output"):
                        for k, res_dt in enumerate(output_dts):
                            del_idx = res_dt.meta_table.get_existing_idx(idx)

                            res_dt.delete_by_idx(del_idx, run_config=run_config)


class BatchTransform(PipelineStep):
    def __init__(
        self,
        func: BatchTransformFunc,
        inputs: List[str],
        outputs: List[str],
        chunk_size: int = 1000,
        **kwargs
    ):
        self.func = func
        self.inputs = inputs
        self.outputs = outputs
        self.chunk_size = chunk_size
        self.kwargs = kwargs

    def build_compute(self, ds: DataStore, catalog: Catalog) -> List[DatatableTransformStep]:
        input_dts = [catalog.get_datatable(ds, name) for name in self.inputs]
        output_dts = [catalog.get_datatable(ds, name) for name in self.outputs]

        def transform_func(
            ds: DataStore,
            input_dts: List[DataTable],
            output_dts: List[DataTable],
            run_config: Optional[RunConfig],
            **kwargs
        ) -> None:
            return batch_transform_wrapper(
                self.func,
                chunksize=self.chunk_size,
                ds=ds,
                input_dts=input_dts,
                output_dts=output_dts,
                run_config=run_config,
                **kwargs
            )

        return [
            DatatableTransformStep(
                f'{self.func.__name__}',  # type: ignore # mypy bug: https://github.com/python/mypy/issues/10976
                input_dts=input_dts,
                output_dts=output_dts,
                func=transform_func,
                **self.kwargs
            )
        ]


class BatchGenerateFunc(Protocol):
    __name__: str
    def __call__(self, **kwargs) -> Iterator[Tuple[DataDF, ...]]: ...


def batch_generate_wrapper(
    func: BatchGenerateFunc,
    ds: DataStore,
    output_dts: List[DataTable],
    run_config: RunConfig = None,
    **kwargs
) -> None:
    import inspect
    import pandas as pd

    '''
    Создание новой таблицы из результатов запуска `proc_func`.
    Функция может быть как обычной, так и генерирующейся
    '''

    now = time.time()
    empty_generator = True

    assert inspect.isgeneratorfunction(func), "Starting v0.8.0 proc_func should be a generator"

    with tracer.start_as_current_span("init generator"):
        try:
            iterable = func(**kwargs)
        except Exception as e:
            logger.exception(f"Generating failed ({func.__name__}): {str(e)}")  # type: ignore # mypy bug: https://github.com/python/mypy/issues/10976
            ds.event_logger.log_exception(e, run_config=run_config)

            # raise e
            return

    while True:
        with tracer.start_as_current_span("get next batch"):
            try:
                chunk_dfs = next(iterable)

                if isinstance(chunk_dfs, pd.DataFrame):
                    chunk_dfs = [chunk_dfs]
            except StopIteration:
                if empty_generator:
                    for k, dt_k in enumerate(output_dts):
                        dt_k.event_logger.log_state(
                            dt_k.name,
                            added_count=0,
                            updated_count=0,
                            deleted_count=0,
                            processed_count=0,
                            run_config=run_config,
                        )

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

    with tracer.start_as_current_span("delete stale rows"):
        for k, dt_k in enumerate(output_dts):
            dt_k.delete_stale_by_process_ts(now, run_config=run_config)


class BatchGenerate(PipelineStep):
    def __init__(
        self,
        func: BatchGenerateFunc,
        outputs: List[str],
        **kwargs
    ):
        self.func = func
        self.outputs = outputs
        self.kwargs = kwargs

    def build_compute(self, ds: DataStore, catalog: Catalog) -> List[DatatableTransformStep]:
        def transform_func(
            ds: DataStore,
            input_dts: List[DataTable],
            output_dts: List[DataTable],
            run_config: Optional[RunConfig],
            **kwargs
        ):
            return batch_generate_wrapper(self.func, ds, output_dts, run_config, **kwargs)

        return [
            DatatableTransformStep(
                name=self.func.__name__,  # type: ignore # mypy bug: https://github.com/python/mypy/issues/10976
                func=transform_func,
                input_dts=[],
                output_dts=[catalog.get_datatable(ds, name) for name in self.outputs],
                check_for_changes=False,
                **self.kwargs,
            )
        ]


def update_external_table(ds: DataStore, table: DataTable, run_config: RunConfig = None) -> None:
    now = time.time()

    for ps_df in tqdm.tqdm(table.table_store.read_rows_meta_pseudo_df(run_config=run_config)):

        (
            new_df,
            changed_df,
            new_meta_df,
            changed_meta_df
        ) = table.meta_table.get_changes_for_store_chunk(ps_df, now=now)

        ds.event_logger.log_state(
            table.name,
            added_count=len(new_df),
            updated_count=len(changed_df),
            deleted_count=0,
            processed_count=len(ps_df),
            run_config=run_config,
        )

        # TODO switch to iterative store_chunk and table.sync_meta_by_process_ts

        table.meta_table.insert_meta_for_store_chunk(new_meta_df)
        table.meta_table.update_meta_for_store_chunk(changed_meta_df)

    for stale_idx in table.meta_table.get_stale_idx(now, run_config=run_config):
        logger.debug(f'Deleting {len(stale_idx.index)} rows from {table.name} data')
        table.event_logger.log_state(
            table.name,
            added_count=0,
            updated_count=0,
            deleted_count=len(stale_idx),
            processed_count=len(stale_idx),
            run_config=run_config,
        )

        table.meta_table.mark_rows_deleted(stale_idx, now=now)


class UpdateExternalTable(PipelineStep):
    def __init__(self, output: str) -> None:
        self.output_table_name = output

    def build_compute(self, ds: DataStore, catalog: Catalog) -> List[DatatableTransformStep]:
        def transform_func(
            ds: DataStore,
            input_dts: List[DataTable],
            output_dts: List[DataTable],
            run_config: Optional[RunConfig],
            **kwargs
        ):
            return update_external_table(ds, output_dts[0], run_config)

        return [
            DatatableTransformStep(
                name=f'update_{self.output_table_name}',
                func=transform_func,
                input_dts=[],
                output_dts=[catalog.get_datatable(ds, self.output_table_name)],
            )
        ]
