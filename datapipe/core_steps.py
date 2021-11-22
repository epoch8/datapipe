from typing import Callable, List, Dict, Iterator, Tuple, Any, Optional, Union
from dataclasses import dataclass

import time
import tqdm
import logging

from datapipe.types import DataDF
from datapipe.compute import PipelineStep, Catalog, ComputeStep
from datapipe.datatable import DataStore, DataTable
from datapipe.run_config import RunConfig


logger = logging.getLogger('datapipe.core_steps')


BatchTransformFunc = Callable[..., Union[DataDF, List[DataDF], Tuple[DataDF, ...]]]


def batch_transform_wrapper(
    func: BatchTransformFunc,
    ds: DataStore,
    input_dts: List[DataTable],
    output_dts: List[DataTable],
    chunksize: int = 1000,
    run_config: RunConfig = None
) -> None:
    import math

    '''
    Множественная инкрементальная обработка `input_dts' на основе изменяющихся индексов
    '''

    idx_count, idx_gen = ds.get_process_ids(
        inputs=input_dts,
        outputs=output_dts,
        chunksize=chunksize,
        run_config=run_config
    )

    logger.info(f'Items to update {idx_count}')

    if idx_count > 0:
        for idx in tqdm.tqdm(idx_gen, total=math.ceil(idx_count / chunksize)):
            logger.debug(f'Idx to process: {idx.to_records()}')

            input_dfs = [inp.get_data(idx) for inp in input_dts]

            if sum(len(j) for j in input_dfs) > 0:
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

                for k, res_dt in enumerate(output_dts):
                    # Берем k-ое значение функции для k-ой таблички
                    # Добавляем результат в результирующие чанки
                    res_dt.store_chunk(
                        data_df=chunks_df[k],
                        processed_idx=idx,
                        run_config=run_config,
                    )

            else:
                for k, res_dt in enumerate(output_dts):
                    res_dt.delete_by_idx(idx, run_config=run_config)


@dataclass
class BatchTransform(PipelineStep):
    func: Callable
    inputs: List[str]
    outputs: List[str]
    chunk_size: int = 1000

    def build_compute(self, ds: DataStore, catalog: Catalog) -> List[ComputeStep]:
        input_dts = [catalog.get_datatable(ds, name) for name in self.inputs]
        output_dts = [catalog.get_datatable(ds, name) for name in self.outputs]

        def transform_func(ds, input_dts, output_dts, run_config):
            return batch_transform_wrapper(
                self.func,
                chunksize=self.chunk_size,
                ds=ds,
                input_dts=input_dts,
                output_dts=output_dts,
                run_config=run_config,
            )

        return [
            DatatableTransformStep(
                f'{self.func.__name__}',
                input_dts=input_dts,
                output_dts=output_dts,
                func=transform_func,
            )
        ]


BatchGenerateFunc = Callable[[], Iterator[Tuple[DataDF, ...]]]


def batch_generate_wrapper(
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

    assert inspect.isgeneratorfunction(func), "Starting v0.8.0 proc_func should be a generator"

    try:
        iterable = func()
    except Exception as e:
        logger.exception(f"Generating failed ({func.__name__}): {str(e)}")
        ds.event_logger.log_exception(e, run_config=run_config)

        # raise e
        return

    while True:
        try:
            chunk_dfs = next(iterable)

            if isinstance(chunk_dfs, pd.DataFrame):
                chunk_dfs = [chunk_dfs]
        except StopIteration:
            break
        except Exception as e:
            logger.exception(f"Generating failed ({func}): {str(e)}")
            ds.event_logger.log_exception(e, run_config=run_config)

            # raise e
            return

        for k, dt_k in enumerate(output_dts):
            dt_k.store_chunk(chunk_dfs[k], run_config=run_config)

    for k, dt_k in enumerate(output_dts):
        dt_k.delete_stale_by_process_ts(now, run_config=run_config)


@dataclass
class BatchGenerate(PipelineStep):
    func: BatchGenerateFunc
    outputs: List[str]

    def build_compute(self, ds: DataStore, catalog: Catalog) -> List[ComputeStep]:
        def transform_func(ds, input_dts, output_dts, run_config):
            return batch_generate_wrapper(self.func, ds, output_dts, run_config)

        return [
            DatatableTransformStep(
                name=self.func.__name__,
                func=transform_func,
                input_dts=[],
                output_dts=[catalog.get_datatable(ds, name) for name in self.outputs],
                check_for_changes=False
            )
        ]


class UpdateExternalTable(PipelineStep):
    def __init__(self, output: str) -> None:
        self.output_table_name = output

    def build_compute(self, ds: DataStore, catalog: Catalog) -> List[ComputeStep]:
        return [
            UpdateExternalTableStep(
                name=f'update_{self.output_table_name}',
                table=catalog.get_datatable(ds, self.output_table_name)
            )
        ]


class UpdateExternalTableStep(ComputeStep):
    def __init__(self, name: str, table: DataTable):
        self._name = name
        self.table = table

    @property
    def name(self) -> str:
        return self._name

    @property
    def input_dts(self) -> List['DataTable']:
        return []

    @property
    def output_dts(self) -> List['DataTable']:
        return [self.table]

    def run(self, ds: DataStore, run_config: RunConfig = None) -> None:
        now = time.time()

        for ps_df in tqdm.tqdm(self.table.table_store.read_rows_meta_pseudo_df(run_config=run_config)):

            _, _, new_meta_df, changed_meta_df = self.table.meta_table.get_changes_for_store_chunk(ps_df, now=now)

            if len(new_meta_df) > 0 or len(changed_meta_df) > 0:
                ds.event_logger.log_state(
                    self.name,
                    added_count=len(new_meta_df),
                    updated_count=len(changed_meta_df),
                    deleted_count=0,
                    run_config=run_config,
                )

            # TODO switch to iterative store_chunk and self.table.sync_meta_by_process_ts

            self.table.meta_table.insert_meta_for_store_chunk(new_meta_df)
            self.table.meta_table.update_meta_for_store_chunk(changed_meta_df)

        for stale_idx in self.table.meta_table.get_stale_idx(now):
            logger.debug(f'Deleting {len(stale_idx.index)} rows from {self.name} data')
            self.output_dts[0].event_logger.log_state(
                self.name,
                added_count=0,
                updated_count=0,
                deleted_count=len(stale_idx),
                run_config=run_config,
            )

            self.table.meta_table.mark_rows_deleted(stale_idx, now=now)


DTTransformFunc = Callable[[DataStore, List[DataTable], List[DataTable], Optional[RunConfig]], None]


class DatatableTransform(PipelineStep):
    def __init__(
        self,
        func: DTTransformFunc,
        inputs: List[str],
        outputs: List[str],
    ) -> None:
        self.func = func
        self.inputs = inputs
        self.outputs = outputs

    def build_compute(self, ds: DataStore, catalog: Catalog) -> List[ComputeStep]:
        return [
            DatatableTransformStep(
                name=self.func.__name__,
                input_dts=[catalog.get_datatable(ds, i) for i in self.inputs],
                output_dts=[catalog.get_datatable(ds, i) for i in self.outputs],
                func=self.func,
            )
        ]


class DatatableTransformStep(ComputeStep):
    def __init__(
        self,
        name: str,
        input_dts: List[DataTable],
        output_dts: List[DataTable],
        func: DTTransformFunc,
        kwargs: Dict[str, Any] = None,
        check_for_changes: bool = True,
    ) -> None:
        self._name = name
        self._input_dts = input_dts
        self._output_dts = output_dts
        self.func = func
        self.kwargs = kwargs or {}
        self.check_for_changes = check_for_changes

    @property
    def name(self) -> str:
        return self._name

    @property
    def input_dts(self) -> List[DataTable]:
        return self._input_dts

    @property
    def output_dts(self) -> List[DataTable]:
        return self._output_dts

    def run(self, ds: DataStore, run_config: RunConfig = None) -> None:
        if len(self.input_dts) > 0 and self.check_for_changes:
            changed_idx_count = ds.get_changed_idx_count(
                inputs=self.input_dts,
                outputs=self.output_dts,
                run_config=run_config
            )

            if changed_idx_count == 0:
                logger.debug(f'Skipping {self.name} execution - nothing to compute')

                return

        run_config = RunConfig.add_labels(run_config, {'step_name': self.name})

        self.func(ds, self._input_dts, self._output_dts, run_config)
