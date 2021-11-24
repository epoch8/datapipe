from typing import Callable, List
from dataclasses import dataclass

import time
import tqdm
import logging

from datapipe.compute import PipelineStep, Catalog, ComputeStep
from datapipe.datatable import DataStore, DataTable, gen_process_many, inc_process_many
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
    chunk_size: int

    def run(self, ds: DataStore, run_config: RunConfig = None) -> None:
        inc_process_many(
            ds,
            self.input_dts,
            self.output_dts,
            self.func,
            self.chunk_size,
            run_config=RunConfig.add_labels(run_config, {'step_name': self.name})
        )


@dataclass
class BatchGenerate(PipelineStep):
    func: Callable
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
    func: Callable

    def run(self, ds: DataStore, run_config: RunConfig = None) -> None:
        gen_process_many(
            self.output_dts,
            self.func,
            run_config=RunConfig.add_labels(run_config, {'step_name': self.name}),
        )


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

            (
                new_df,
                changed_df,
                new_meta_df,
                changed_meta_df
             ) = self.table.meta_table.get_changes_for_store_chunk(ps_df, now=now)

            ds.event_logger.log_state(
                self.name,
                added_count=len(new_df),
                updated_count=len(changed_df),
                deleted_count=0,
                processed_count=len(ps_df),
                run_config=run_config,
            )

            # TODO switch to iterative store_chunk and self.table.sync_meta_by_process_ts

            self.table.meta_table.insert_meta_for_store_chunk(new_meta_df)
            self.table.meta_table.update_meta_for_store_chunk(changed_meta_df)

        for stale_idx in self.table.meta_table.get_stale_idx(now, run_config=run_config):
            logger.debug(f'Deleting {len(stale_idx.index)} rows from {self.name} data')
            self.output_dts[0].event_logger.log_state(
                self.name,
                added_count=0,
                updated_count=0,
                deleted_count=len(stale_idx),
                processed_count=len(stale_idx),
                run_config=run_config,
            )

            self.table.meta_table.mark_rows_deleted(stale_idx, now=now)
