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


class SqlQueryRequest(BaseModel):
    sql: str
    limit: Optional[int] = 1000
    offset: Optional[int] = 0
    datasource: Optional[str] = "datapipe_analytics"


class SqlQueryResponse(BaseModel):
    columns: list[dict[str, str]]
    rows: list[dict[str, object]]
    total: Optional[int] = None
    execution_ms: float
    truncated: bool = False


class SqlSchemaResponse(BaseModel):
    datasource: str
    tables: list[dict[str, object]]
