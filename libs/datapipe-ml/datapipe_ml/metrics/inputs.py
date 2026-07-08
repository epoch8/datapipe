from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import List

import pandas as pd
from datapipe.datatable import DataStore, DataTable
from datapipe.types import PipelineInput
from sqlalchemy import Column

from datapipe_ml.core.datapipe import PipelineInputOrList, get_datatable, normalize_pipeline_inputs
from datapipe_ml.metrics.common import stable_unique
from datapipe_ml.utils.pipeline_inputs import (
    build_required_pipeline_inputs,
    merge_inputs_on_keys,
    primary_pipeline_input,
)


def wrap_ground_truth_inputs(
    func: Callable,
    *,
    n_ground_truth_inputs: int,
    primary_keys: List[str],
) -> Callable:
    if n_ground_truth_inputs < 1:
        raise ValueError("n_ground_truth_inputs must be at least 1")

    def wrapped(*dfs, **kwargs):
        gt_df = merge_inputs_on_keys(dfs[:n_ground_truth_inputs], primary_keys)
        return func(gt_df, *dfs[n_ground_truth_inputs:], **kwargs)

    return wrapped


def get_ground_truth_datatables(ds: DataStore, input__image__ground_truth: PipelineInputOrList) -> List[DataTable]:
    return [get_datatable(ds, table) for table in normalize_pipeline_inputs(input__image__ground_truth)]


def merged_ground_truth_primary_schema(
    datatables: Sequence[DataTable],
    *,
    exclude_columns: Sequence[str] = ("subset_id",),
) -> List[Column]:
    seen: set[str] = set()
    columns: List[Column] = []
    for dt in datatables:
        for column in dt.primary_schema:
            if column.name in exclude_columns or column.name in seen:
                continue
            seen.add(column.name)
            columns.append(column)
    return columns


def ground_truth_convert_keys(
    primary_keys: List[str],
    model_primary_keys: List[str],
    df__ground_truth: pd.DataFrame,
) -> List[str]:
    extra = [key for key in model_primary_keys if key not in primary_keys and key in df__ground_truth.columns]
    return stable_unique(primary_keys + ["subset_id"] + extra)


def prediction_convert_keys(
    primary_keys: List[str],
    model_primary_keys: List[str],
    df__prediction: pd.DataFrame,
) -> List[str]:
    model_keys = [key for key in model_primary_keys if key in df__prediction.columns]
    return stable_unique(primary_keys + model_keys + ["subset_id"])


def build_ground_truth_batch_inputs(input__image__ground_truth: PipelineInputOrList) -> list[PipelineInput]:
    return build_required_pipeline_inputs(input__image__ground_truth)


def primary_ground_truth_input(input__image__ground_truth: PipelineInputOrList) -> PipelineInput:
    return primary_pipeline_input(input__image__ground_truth)


def _table_column_names(ds: DataStore, table: PipelineInput) -> set[str]:
    from datapipe.store.database import TableStoreDB
    from datapipe.store.filedir import TableStoreFiledir

    datatable = get_datatable(ds, table)
    table_store = datatable.table_store
    if isinstance(table_store, TableStoreDB):
        return {column.name for column in table_store.data_sql_schema}
    if isinstance(table_store, TableStoreFiledir):
        columns = set(table_store.attrnames)
        if table_store.add_filepath_column:
            columns.add("filepath")
        return columns
    raise ValueError(f"Unknown table_store: {table_store=}")


def model_primary_keys_in_table(
    ds: DataStore,
    table: PipelineInput,
    model_primary_keys: List[str],
) -> List[str]:
    columns = _table_column_names(ds, table)
    return [key for key in model_primary_keys if key in columns]


def model_primary_key_columns(
    ds: DataStore,
    model_primary_keys: List[str],
    *,
    prediction: PipelineInput,
    ground_truth: PipelineInputOrList,
) -> List[Column]:
    tables = [get_datatable(ds, prediction), *get_ground_truth_datatables(ds, ground_truth)]
    columns: List[Column] = []
    seen: set[str] = set()
    for key in model_primary_keys:
        for datatable in tables:
            for column in datatable.primary_schema:
                if column.name == key and key not in seen:
                    columns.append(column)
                    seen.add(key)
                    break
            if key in seen:
                break
        if key not in seen:
            raise ValueError(f"Missing model primary key column {key!r} in prediction/ground-truth tables")
    return columns
