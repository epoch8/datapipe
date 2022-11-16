# flake8: noqa

import time
from typing import List
import numpy as np
import pandas as pd

from typing import cast
from datapipe.types import IndexDF
from datapipe.datatable import DataStore, DataTable, RunConfig
from datapipe.datatable import DataStore

def cross_merge_df(
    ds: DataStore,
    input_dts: List[DataTable],
    output_dts: List[DataTable],
    run_config: RunConfig,
    chunk_size: int = 100
):
    # Input: 1st ['id_R', 'data_R'] of left
    # Input: 2nd ['id_L', 'data_L'] of right
    # Output: Cartesian product [(1st X 2nd), 'data_R', 'data_L']
    assert len(input_dts) == 2
    dt_left = input_dts[0]
    dt_right = input_dts[1]
    dt_left_x_right = output_dts[0]
    changed_idx_left_count = ds.get_changed_idx_count([dt_left], [dt_left_x_right])
    changed_idx_right_count = ds.get_changed_idx_count([dt_right], [dt_left_x_right])
    print(f"Items to update: left: {changed_idx_left_count}, right: {changed_idx_right_count}")
    
    now = time.time()
    idxs_left = dt_left.meta_table.get_existing_idx()
    idxs_right = dt_right.meta_table.get_existing_idx()
    for idx_left in np.array_split(idxs_left, max(1, len(idxs_left) // chunk_size)):
        df_left = dt_left.get_data(idx=idx_left)
        for idx_right in np.array_split(idxs_right, max(1, len(idxs_right) // chunk_size)):
            df_right = dt_right.get_data(idx=idx_right)
            dt_left_x_right.store_chunk(pd.merge(df_left, df_right, how='cross'), run_config=run_config)

    dt_left_x_right.delete_stale_by_process_ts(now, run_config=run_config)
