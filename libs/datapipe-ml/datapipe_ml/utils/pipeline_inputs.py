from __future__ import annotations

from collections.abc import Sequence
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


def primary_pipeline_input(inputs: PipelineInputOrList) -> PipelineInput:
    return normalize_pipeline_inputs(inputs)[0]
