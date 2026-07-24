from __future__ import annotations

from typing import Optional

from datapipe.types import Labels
from pydantic import BaseModel


class StartRunRequest(BaseModel):
    labels: Labels = []
    background: bool = True


class StartRunResponse(BaseModel):
    run_id: str
    status: str = "running"
