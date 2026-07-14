from typing import Any, Dict, List, Literal, Optional, Union

from pydantic import BaseModel, Field


class PipelineStepResponse(BaseModel):
    name: str

    type_: Literal["transform"] = Field(alias="type", default="transform")
    transform_type: str

    indexes: Optional[List[str]] = None

    inputs: List[str]
    outputs: List[str]

    labels: List[List[str]] = Field(default_factory=list)

    has_transform_meta: bool = False
    total_idx_count: Optional[int] = None
    changed_idx_count: Optional[int] = None


class MetaPipelineStepResponse(BaseModel):
    name: str

    type_: Literal["meta"] = Field(alias="type")
    transform_type: str = ""
    inputs: List[str] = Field(default_factory=list)
    outputs: List[str] = Field(default_factory=list)
    labels: List[List[str]] = Field(default_factory=list)
    graph: "GraphResponse"


PipelineNodeResponse = Union[PipelineStepResponse, MetaPipelineStepResponse]


class TableResponse(BaseModel):
    name: str

    indexes: List[str]

    size: Optional[int] = None
    store_class: str
    schema_: List["TableColumnResponse"] = Field(alias="schema", default_factory=list)


class TableColumnResponse(BaseModel):
    name: str
    type: str


class GraphResponse(BaseModel):
    catalog: Dict[str, TableResponse]
    pipeline: List[PipelineNodeResponse]
    stages: List[str] = Field(default_factory=list)


TableResponse.model_rebuild()
GraphResponse.model_rebuild()
MetaPipelineStepResponse.model_rebuild()


class FocusFilter(BaseModel):
    table_name: str
    items_idx: List[Dict]


class GetDataRequest(BaseModel):
    table: str
    filters: Dict[str, Any] = {}
    page: int = 0
    page_size: int = 5
    order_by: Optional[str] = None
    order: Literal["asc", "desc"] = "asc"
    focus: Optional[FocusFilter] = None
    include_total: bool = False


class GetDataResponse(BaseModel):
    page: int
    page_size: int
    total: Optional[int] = None
    data: List[Dict]


class TableSizeResponse(BaseModel):
    table: str
    size: int


class RunStepRequest(BaseModel):
    transform: str
    operation: Literal["run-step"]
    filters: Optional[List[Dict]] = None


class RunStepResponse(BaseModel):
    status: Literal["starting", "running", "finished"]
    processed: int
    total: int
    run_id: Optional[str] = None
