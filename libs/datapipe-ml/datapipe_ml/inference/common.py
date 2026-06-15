from __future__ import annotations

from typing import Optional

import pandas as pd


def resolve_threshold_space(
    *,
    threshold_space: Optional[int] = None,
    thresehold_space: Optional[int] = None,
) -> Optional[int]:
    if threshold_space is not None and thresehold_space is not None and threshold_space != thresehold_space:
        raise ValueError("thresholdSpace and deprecated threseholdSpace disagree")
    return threshold_space if threshold_space is not None else thresehold_space


def min_prediction_threshold_from_class_thresholds(
    df: pd.DataFrame,
    class_name_to_threshold__name: str,
    *,
    default: float = 0.01,
) -> float:
    mins: list[float] = []
    for mapping in df[class_name_to_threshold__name]:
        if isinstance(mapping, dict) and mapping:
            mins.append(min(float(value) for value in mapping.values()))
    return min(mins) if mins else default
