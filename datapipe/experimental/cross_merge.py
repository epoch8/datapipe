# flake8: noqa

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
    run_config: RunConfig
):
    # Input: 1st ['id_R', 'data_R'] of left
    # Input: 2nd ['id_L', 'data_L'] of right
    # Output: Cartesian product [(1st X 2nd), 'data_R', 'data_L']
    dt_left = input_dts[0]
    dt_right = input_dts[1]
    dt_left_x_right = output_dts[0]
    changed_idx_left_count = ds.get_changed_idx_count([dt_left], [dt_left_x_right])
    changed_idx_right_count = ds.get_changed_idx_count([dt_right], [dt_left_x_right])
    print(f"Items to update: left: {changed_idx_left_count}, right: {changed_idx_right_count}")

    # Changed df_left [TODO: реагировать на удаление]
    right_idxs = dt_right.meta_table.get_existing_idx()
    if changed_idx_left_count > 0:
        _, idx_gen_left = ds.get_full_process_ids([dt_left], [dt_left_x_right])
        for changed_idx_left in idx_gen_left:
            df_left = dt_left.get_data(idx=changed_idx_left)
            # Шаг 1: подхватывать измененные индексы относительно таблицы:
            if changed_idx_right_count > 0:
                _, changed_idx_right = ds.get_full_process_ids([dt_right], [dt_left_x_right])
                for idx_right in changed_idx_right:
                    df_right = dt_right.get_data(idx=idx_right)
                    if len(df_right) > 0:
                        df_left_x_right = pd.merge(df_left, df_right, how='cross')
                        processed_idx = pd.merge(changed_idx_left, idx_right, how='cross')
                        dt_left_x_right.store_chunk(df_left_x_right, processed_idx=processed_idx)
                    else:
                        del_idx = dt_left_x_right.meta_table.get_existing_idx(idx_right)
                        dt_left_x_right.delete_by_idx(del_idx)

                    # Шаг 3: подхватывать всю табличку left для новых right
                    left_idxs = dt_left.meta_table.get_existing_idx()
                    for left_idx_chunk in np.array_split(left_idxs, max(1, len(left_idxs) // 1000)):
                        df_left_chunk = dt_left.get_data(idx=cast(IndexDF, left_idx_chunk))
                        df_left_x_right = pd.merge(df_left_chunk, df_right, how='cross')
                        dt_left_x_right.store_chunk(df_left_x_right)

            # Шаг 2: подхватывать всю табличку right для новых left
            right_idxs = dt_right.meta_table.get_existing_idx()
            for right_idx_chunk in np.array_split(right_idxs, max(1, len(right_idxs) // 1000)):
                df_right = dt_right.get_data(idx=cast(IndexDF, right_idx_chunk))
                df_left_x_right = pd.merge(df_left, df_right, how='cross')
                dt_left_x_right.store_chunk(df_left_x_right)
    elif changed_idx_right_count > 0:
        # Случай по сути симметричный, поэтому
        cross_merge_df(
            ds=ds,
            input_dts=input_dts[::-1],
            output_dts=output_dts[::-1],
            run_config=run_config
        )
