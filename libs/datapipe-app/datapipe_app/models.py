from typing import Any, Literal

from pydantic import BaseModel, Field


class PipelineStepResponse(BaseModel):
    name: str

    type_: str = Field(alias="type")
    transform_type: str

    indexes: list[str] | None = None

    inputs: list[str]
    outputs: list[str]

    total_idx_count: int | None = None
    changed_idx_count: int | None = None


class TableResponse(BaseModel):
    name: str

    indexes: list[str]

    size: int
    store_class: str


class GraphResponse(BaseModel):
    catalog: dict[str, TableResponse]
    pipeline: list[PipelineStepResponse]


class FocusFilter(BaseModel):
    table_name: str
    items_idx: list[dict]


class GetDataRequest(BaseModel):
    table: str
    filters: dict[str, Any] = {}
    page: int = 0
    page_size: int = 20
    order_by: str | None = None
    order: Literal["asc", "desc"] = "asc"
    focus: FocusFilter | None = None


class GetDataResponse(BaseModel):
    page: int
    page_size: int
    total: int
    data: list[dict]


class RunStepRequest(BaseModel):
    transform: str
    operation: Literal["run-step"]
    filters: list[dict] | None = None


class RunStepResponse(BaseModel):
    status: Literal["starting", "running", "finished"]
    processed: int
    total: int
