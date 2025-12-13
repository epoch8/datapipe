import logging
import time
from typing import List, Optional

import pandas as pd
from tqdm_loggable.auto import tqdm

from datapipe.compute import Catalog, ComputeStep, PipelineStep
from datapipe.datatable import DataStore, DataTable
from datapipe.run_config import RunConfig
from datapipe.step.datatable_transform import (
    DatatableTransformFunc,
    DatatableTransformStep,
)
from datapipe.types import Labels, MetadataDF, TableOrName, cast

logger = logging.getLogger("datapipe.step.update_external_table")


def update_external_table(ds: DataStore, table: DataTable, run_config: Optional[RunConfig] = None) -> None:
    now = time.time()

    for ps_df in tqdm(table.table_store.read_rows_meta_pseudo_df(run_config=run_config)):
        (
            new_index_df,
            changed_index_df,
            new_meta_df,
            changed_meta_df,
        ) = table.meta.get_changes_for_store_chunk(table.table_store.hash_rows(ps_df), now=now)

        # TODO switch to iterative store_chunk and table.sync_meta_by_process_ts

        table.meta.update_rows(
            cast(
                MetadataDF,
                pd.concat(df for df in [new_meta_df, changed_meta_df] if not df.empty),
            ),
        )

    for stale_idx in table.meta.get_stale_idx(now, run_config=run_config):
        logger.debug(f"Deleting {len(stale_idx.index)} rows from {table.name} data")
        table.event_logger.log_state(
            table.name,
            added_count=0,
            updated_count=0,
            deleted_count=len(stale_idx),
            processed_count=len(stale_idx),
            run_config=run_config,
        )

        table.meta.mark_rows_deleted(stale_idx, now=now)


class UpdateExternalTable(PipelineStep):
    def __init__(
        self,
        output: TableOrName,
        labels: Optional[Labels] = None,
    ) -> None:
        self.output_table_name = output
        self.labels = labels

    def build_compute(self, ds: DataStore, catalog: Catalog) -> List[ComputeStep]:
        def transform_func(
            ds: DataStore,
            input_dts: List[DataTable],
            output_dts: List[DataTable],
            run_config: Optional[RunConfig],
            **kwargs,
        ):
            return update_external_table(ds, output_dts[0], run_config)

        output_table = catalog.get_datatable(ds, self.output_table_name)

        return [
            DatatableTransformStep(
                name=f"update_{output_table.name}",
                func=cast(DatatableTransformFunc, transform_func),
                input_dts=[],
                output_dts=[output_table],
                labels=self.labels,
            )
        ]
