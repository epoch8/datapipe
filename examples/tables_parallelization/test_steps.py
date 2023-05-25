from typing import Any, Dict, List, Optional
from datapipe.types import ChangeList
from datapipe.run_config import RunConfig
from datapipe.compute import (
    ComputeStep,
)
from datapipe.datatable import DataStore

import pandas as pd
import time


def stepAB(dfA: pd.DataFrame) -> pd.DataFrame:
    df = dfA.copy()
    df["col"] = df["col"] + "_AB"
    time.sleep(3)
    return df


def stepBD(dfB: pd.DataFrame) -> pd.DataFrame:
    df = dfB.copy()
    df["col"] = df["col"] + "_BD"
    time.sleep(3)
    return df


def stepFC(dfF: pd.DataFrame) -> pd.DataFrame:
    df = dfF.copy()
    df["col"] = df["col"] + "_FC"
    time.sleep(3)
    return df


def stepBCE(dfB: pd.DataFrame, dfC: pd.DataFrame) -> pd.DataFrame:
    merged_df = dfB.merge(dfC, on="id")
    merged_df["col"] = merged_df[["col_x", "col_y"]].apply(
        lambda x: "___".join(x.dropna()), axis=1
    )
    merged_df.drop(["col_x", "col_y"], axis=1, inplace=True)
    time.sleep(3)
    return merged_df
