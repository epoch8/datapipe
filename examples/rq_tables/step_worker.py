from typing import Any, Dict, List, Optional
from datapipe.types import ChangeList
from datapipe.run_config import RunConfig
from datapipe.compute import (
    ComputeStep,
)
from datapipe.datatable import DataStore


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
