from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field


class PipelineStepResponse(BaseModel):
    name: str

    type_: str = Field(alias="type")
    transform_type: str

    indexes: Optional[List[str]] = None

    inputs: List[str]
    outputs: List[str]

    total_idx_count: Optional[int] = None
    changed_idx_count: Optional[int] = None


class TableResponse(BaseModel):
    name: str

    indexes: List[str]

    size: int
    store_class: str


class GraphResponse(BaseModel):
    catalog: Dict[str, TableResponse]
    pipeline: List[PipelineStepResponse]


class FocusFilter(BaseModel):
    table_name: str
    items_idx: List[Dict]


class GetDataRequest(BaseModel):
    table: str
    filters: Dict[str, Any] = {}
    page: int = 0
    page_size: int = 20
    order_by: Optional[str] = None
    order: Literal["asc", "desc"] = "asc"
    focus: Optional[FocusFilter] = None


class GetDataResponse(BaseModel):
    page: int
    page_size: int
    total: int
    data: List[Dict]


class RunStepRequest(BaseModel):
    transform: str
    operation: Literal["run-step"]
    filters: Optional[List[Dict]] = None


class RunStepResponse(BaseModel):
    status: Literal["starting", "running", "finished"]
    processed: int
    total: int
