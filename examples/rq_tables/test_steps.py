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
    df = dfB.copy()
    df["col"] = dfB["col"] + "___" + dfC["col"] + "_BCE"
    time.sleep(3)
    return df


def step_worker(
    step: ComputeStep,
    ds: DataStore,
    steps: List[ComputeStep],
    changelist: ChangeList,
    run_config: Optional[RunConfig] = None,
):
    print(step.name)

    step_changes = step.run_changelist(ds, changelist, run_config)
    return step_changes
