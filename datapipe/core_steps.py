from typing import Callable, Iterable, List, Iterator, Tuple, Union, Dict, Any
from dataclasses import dataclass

import time
import tqdm
import logging
from opentelemetry import trace

from datapipe.types import DataDF, IndexDF
from datapipe.compute import ComputeStep, PipelineStep, Catalog, DatatableTransformStep
from datapipe.datatable import DataStore, DataTable
from datapipe.run_config import RunConfig
from datapipe.types import ChangeList


logger = logging.getLogger('datapipe.core_steps')
tracer = trace.get_tracer("datapipe.core_steps")


BatchTransformFunc = Callable[..., Union[DataDF, List[DataDF]]]


def do_batch_transform(
    func: BatchTransformFunc,
    ds: DataStore,
    input_dts: List[DataTable],
    output_dts: List[DataTable],
    idx_gen: Iterable[IndexDF],
    idx_count: int = None,
    run_config: RunConfig = None,
) -> Iterator[ChangeList]:
    '''
    Множественная инкрементальная обработка `input_dts' на основе изменяющихся индексов
    '''

    logger.info(f'Items to update {idx_count}')

    if idx_count is not None and idx_count == 0:
        # Nothing to process
        return [ChangeList()]

    for idx in tqdm.tqdm(idx_gen, total=idx_count):
        with tracer.start_as_current_span("process batch"):
            logger.debug(f'Idx to process: {idx.to_records()}')

            with tracer.start_as_current_span("get input data"):
                input_dfs = [inp.get_data(idx) for inp in input_dts]

            changes = ChangeList()

            if sum(len(j) for j in input_dfs) > 0:
                with tracer.start_as_current_span("run transform"):
                    try:
                        chunks_df = func(*input_dfs)
                    except Exception as e:
                        logger.error(f"Transform failed ({func.__name__}): {str(e)}")
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
                        change_idx = res_dt.store_chunk(
                            data_df=chunks_df[k],
                            processed_idx=idx,
                            run_config=run_config,
                        )

                        changes.append(res_dt.name, change_idx)

            else:
                with tracer.start_as_current_span("delete missing data from output"):
                    for k, res_dt in enumerate(output_dts):
                        del_idx = res_dt.meta_table.get_existing_idx(idx)

                        res_dt.delete_by_idx(del_idx, run_config=run_config)

                        changes.append(res_dt.name, del_idx)

            yield changes


def do_full_batch_transform(
    func: BatchTransformFunc,
    ds: DataStore,
    input_dts: List[DataTable],
    output_dts: List[DataTable],
    chunk_size: int = 1000,
    run_config: RunConfig = None,
) -> None:
    with tracer.start_as_current_span("compute ids to process"):
        idx_count, idx_gen = ds.get_full_process_ids(
            inputs=input_dts,
            outputs=output_dts,
            chunk_size=chunk_size,
            run_config=run_config
        )

    gen = do_batch_transform(
        func,
        ds=ds,
        idx_count=idx_count,
        idx_gen=idx_gen,
        input_dts=input_dts,
        output_dts=output_dts,
        run_config=run_config,
    )

    for changes in gen:
        pass


@dataclass
class BatchTransform(PipelineStep):
    func: Callable
    inputs: List[str]
    outputs: List[str]
    chunk_size: int = 1000

    def build_compute(self, ds: DataStore, catalog: Catalog) -> List[ComputeStep]:
        input_dts = [catalog.get_datatable(ds, name) for name in self.inputs]
        output_dts = [catalog.get_datatable(ds, name) for name in self.outputs]

        return [
            BatchTransformStep(
                f'{self.func.__name__}',
                input_dts=input_dts,
                output_dts=output_dts,
                func=self.func,
                chunk_size=self.chunk_size,
            )
        ]


class BatchTransformStep(ComputeStep):
    name: str
    input_dts: List[DataTable]
    output_dts: List[DataTable]

    def __init__(
        self,
        name: str,
        input_dts: List[DataTable],
        output_dts: List[DataTable],

        func: BatchTransformFunc,
        kwargs: Dict[str, Any] = None,
        chunk_size: int = 1000,
    ) -> None:
        self.name = name
        self.input_dts = input_dts
        self.output_dts = output_dts

        self.func = func
        self.kwargs: Dict[str, Any] = kwargs or {}
        self.chunk_size = chunk_size

    def get_name(self) -> str:
        return self.name

    def get_input_dts(self) -> List[DataTable]:
        return self.input_dts

    def get_output_dts(self) -> List[DataTable]:
        return self.output_dts

    def run_full(self, ds: DataStore, run_config: RunConfig = None) -> None:
        run_config = RunConfig.add_labels(run_config, {'step_name': self.name})

        idx_count, idx_gen = ds.get_full_process_ids(
            inputs=self.input_dts,
            outputs=self.output_dts,
            chunk_size=self.chunk_size,
            run_config=run_config
        )

        gen = do_batch_transform(
            self.func,
            ds=ds,
            idx_count=idx_count,
            idx_gen=idx_gen,
            input_dts=self.input_dts,
            output_dts=self.output_dts,
            run_config=run_config,
        )

        for changes in gen:
            pass

    def run_changelist(self, ds: DataStore, change_list: ChangeList, run_config: RunConfig = None) -> ChangeList:
        run_config = RunConfig.add_labels(run_config, {'step_name': self.name})

        idx_count, idx_gen = ds.get_change_list_process_ids(
            inputs=self.input_dts,
            outputs=self.output_dts,
            change_list=change_list,
            chunk_size=self.chunk_size,
            run_config=run_config
        )

        gen = do_batch_transform(
            self.func,
            ds=ds,
            input_dts=self.input_dts,
            output_dts=self.output_dts,
            idx_count=idx_count,
            idx_gen=idx_gen,
            run_config=run_config,
        )

        res_changelist = ChangeList()

        for changes in gen:
            res_changelist.extend(changes)

        return res_changelist


BatchGenerateFunc = Callable[[], Iterator[Tuple[DataDF, ...]]]


def do_batch_generate(
    func: BatchGenerateFunc,
    ds: DataStore,
    output_dts: List[DataTable],
    run_config: RunConfig = None
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
            iterable = func()
        except Exception as e:
            logger.exception(f"Generating failed ({func.__name__}): {str(e)}")
            ds.event_logger.log_exception(e, run_config=run_config)

            raise e

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


@dataclass
class BatchGenerate(PipelineStep):
    func: BatchGenerateFunc
    outputs: List[str]

    def build_compute(self, ds: DataStore, catalog: Catalog) -> List[ComputeStep]:
        def transform_func(ds, input_dts, output_dts, run_config):
            return do_batch_generate(self.func, ds, output_dts, run_config)

        return [
            DatatableTransformStep(
                name=self.func.__name__,
                func=transform_func,
                input_dts=[],
                output_dts=[catalog.get_datatable(ds, name) for name in self.outputs],
                check_for_changes=False
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

    def build_compute(self, ds: DataStore, catalog: Catalog) -> List[ComputeStep]:
        def transform_func(ds, input_dts, output_dts, run_config):
            return update_external_table(ds, output_dts[0], run_config)

        return [
            DatatableTransformStep(
                name=f'update_{self.output_table_name}',
                func=transform_func,
                input_dts=[],
                output_dts=[catalog.get_datatable(ds, self.output_table_name)],
            )
        ]
