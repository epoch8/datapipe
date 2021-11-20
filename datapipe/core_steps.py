from typing import Callable, List, Dict, Iterator, Tuple, Any
from dataclasses import dataclass, field

import time
import tqdm
import logging

from datapipe.types import DataDF
from datapipe.compute import PipelineStep, Catalog, ComputeStep
from datapipe.datatable import DataStore, DataTable
from datapipe.run_config import RunConfig


logger = logging.getLogger('datapipe.core_steps')


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
            BatchTransformIncStep(
                f'{self.func.__name__}',
                input_dts=input_dts,
                output_dts=output_dts,
                func=self.func,
                chunk_size=self.chunk_size
            )
        ]


@dataclass
class BatchTransformIncStep(ComputeStep):
    func: Callable
    kwargs: Dict[str, Any] = field(default_factory=dict)
    chunk_size: int = 1000

    def run(self, ds: DataStore, run_config: RunConfig = None) -> None:
        import math

        input_dts = self.input_dts
        res_dts = self.output_dts
        proc_func = self.func
        kwargs = self.kwargs
        chunksize = self.chunk_size
        run_config = RunConfig.add_labels(run_config, {'step_name': self.name})

        '''
        Множественная инкрементальная обработка `input_dts' на основе изменяющихся индексов
        '''

        idx_count, idx_gen = ds.get_process_ids(
            inputs=input_dts,
            outputs=res_dts,
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
                        chunks_df = proc_func(*input_dfs, **kwargs)
                    except Exception as e:
                        logger.error(f"Transform failed ({proc_func.__name__}): {str(e)}")
                        ds.event_logger.log_exception(e, run_config=run_config)

                        continue

                    for k, res_dt in enumerate(res_dts):
                        # Берем k-ое значение функции для k-ой таблички
                        chunk_df_k = chunks_df[k] if len(res_dts) > 1 else chunks_df

                        # Добавляем результат в результирующие чанки
                        res_dt.store_chunk(
                            data_df=chunk_df_k,
                            processed_idx=idx,
                            run_config=run_config,
                        )

                else:
                    for k, res_dt in enumerate(res_dts):
                        res_dt.delete_by_idx(idx, run_config=run_config)


@dataclass
class BatchGenerate(PipelineStep):
    func: Callable[
        ...,
        Iterator[Tuple[DataDF, ...]]
    ]
    outputs: List[str]

    def build_compute(self, ds: DataStore, catalog: Catalog) -> List[ComputeStep]:
        output_dts = [catalog.get_datatable(ds, name) for name in self.outputs]

        return [
            BatchGenerateStep(
                f'{self.func.__name__}',
                input_dts=[],
                output_dts=output_dts,
                func=self.func,
            )
        ]


@dataclass
class BatchGenerateStep(ComputeStep):
    func: Callable[
        ...,
        Iterator[Tuple[DataDF, ...]]
    ]
    kwargs: Dict[str, Any] = field(default_factory=dict)

    def run(self, ds: DataStore, run_config: RunConfig = None) -> None:
        import inspect
        import pandas as pd

        dts = self.output_dts
        proc_func = self.func
        kwargs = self.kwargs
        run_config = RunConfig.add_labels(run_config, {'step_name': self.name})

        '''
        Создание новой таблицы из результатов запуска `proc_func`.
        Функция может быть как обычной, так и генерирующейся
        '''

        now = time.time()

        assert inspect.isgeneratorfunction(proc_func), "Starting v0.8.0 proc_func should be a generator"

        try:
            iterable = proc_func(**kwargs)
        except Exception as e:
            logger.exception(f"Generating failed ({proc_func.__name__}): {str(e)}")
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
                logger.exception(f"Generating failed ({proc_func.__name__}): {str(e)}")
                ds.event_logger.log_exception(e, run_config=run_config)

                # raise e
                return

            for k, dt_k in enumerate(dts):
                dt_k.store_chunk(chunk_dfs[k], run_config=run_config)

        for k, dt_k in enumerate(dts):
            dt_k.delete_stale_by_process_ts(now, run_config=run_config)


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
        self.name = name
        self.table = table
        self.input_dts = []
        self.output_dts = [table]

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
