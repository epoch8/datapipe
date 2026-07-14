from __future__ import annotations

from typing import Optional

from pydantic import BaseModel


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
