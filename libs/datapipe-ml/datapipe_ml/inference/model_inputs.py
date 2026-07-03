from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import List

import pandas as pd
from datapipe.types import PipelineInput, required_pipeline_input

from datapipe_ml.core.datapipe import PipelineInputOrList, normalize_pipeline_inputs


def _duplicate_non_key_columns(
    left: pd.DataFrame,
    right: pd.DataFrame,
    primary_keys: List[str],
) -> set[str]:
    key_columns = set(primary_keys)
    left_columns = set(left.columns) - key_columns
    right_columns = set(right.columns) - key_columns
    return left_columns & right_columns


def merge_inputs_on_keys(
    dfs: Sequence[pd.DataFrame],
    primary_keys: List[str],
) -> pd.DataFrame:
    if not dfs:
        raise ValueError("dfs must contain at least one dataframe")
    merged = dfs[0]
    for filter_df in dfs[1:]:
        if filter_df.empty:
            continue
        duplicate_columns = _duplicate_non_key_columns(merged, filter_df, primary_keys)
        if duplicate_columns:
            raise ValueError(
                f"Input tables share non-key columns {sorted(duplicate_columns)}; "
                f"only primary keys {primary_keys} may overlap across tables"
            )
        merged = merged.merge(filter_df, on=primary_keys)
    return merged


def build_required_pipeline_inputs(inputs: PipelineInputOrList) -> list[PipelineInput]:
    tables = normalize_pipeline_inputs(inputs)
    if not tables:
        raise ValueError("inputs must contain at least one table")
    return [required_pipeline_input(table) for table in tables]


def primary_model_input(input__model: PipelineInputOrList) -> PipelineInput:
    return normalize_pipeline_inputs(input__model)[0]


def wrap_inference_inputs(
    func: Callable,
    *,
    n_image_inputs: int,
    primary_keys: List[str],
    model_input_groups: Sequence[tuple[int, List[str]]],
    n_trailing_inputs: int = 0,
) -> Callable:
    if not model_input_groups:
        raise ValueError("model_input_groups must contain at least one group")

    def wrapped(*dfs, **kwargs):
        offset = n_image_inputs
        image_df = merge_inputs_on_keys(dfs[:offset], primary_keys)

        model_dfs = []
        for count, model_primary_keys in model_input_groups:
            end = offset + count
            model_dfs.append(merge_inputs_on_keys(dfs[offset:end], model_primary_keys))
            offset = end

        trailing_dfs = dfs[offset : offset + n_trailing_inputs]
        return func(image_df, *model_dfs, *trailing_dfs, **kwargs)

    return wrapped
