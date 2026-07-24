from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import List

from datapipe.types import PipelineInput

from datapipe_ml.core.datapipe import PipelineInputOrList
from datapipe_ml.utils.pipeline_inputs import (
    build_required_pipeline_inputs,
    merge_inputs_on_keys,
    primary_pipeline_input,
)

__all__ = [
    "build_required_pipeline_inputs",
    "merge_inputs_on_keys",
    "primary_model_input",
    "wrap_inference_inputs",
]


def primary_model_input(input__model: PipelineInputOrList) -> PipelineInput:
    return primary_pipeline_input(input__model)


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
