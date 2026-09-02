from typing import Any, Dict, List, Literal, Optional, Union

from pydantic import BaseModel, ConfigDict, Field


class PipelineStepResponse(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

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
    model_config = ConfigDict(populate_by_name=True)

    name: str

    type_: Literal["meta"] = Field(alias="type")
    transform_type: str = ""
    inputs: List[str] = Field(default_factory=list)
    outputs: List[str] = Field(default_factory=list)
    labels: List[List[str]] = Field(default_factory=list)
    graph: "GraphResponse"


PipelineNodeResponse = Union[PipelineStepResponse, MetaPipelineStepResponse]


class TableColumnResponse(BaseModel):
    name: str
    type: str


class TableResponse(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    name: str

    indexes: List[str]

    size: Optional[int] = None
    store_class: str
    schema_: List[TableColumnResponse] = Field(alias="schema", default_factory=list)


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


class TableSizeResponse(BaseModel):
    table: str
    size: int


class AddonCapability(BaseModel):
    """Opaque capability bag contributed by an external addon library."""

    name: str
    features: Dict[str, Any] = Field(default_factory=dict)


class CapabilitiesResponse(BaseModel):
    addons: List[AddonCapability] = Field(default_factory=list)


class SettingsResponse(BaseModel):
    version: str


class ResetTransformMetadataResponse(BaseModel):
    transform_name: str
    status: str = "ok"


class LabelSegment(BaseModel):
    label_id: str
    start_order: int
    end_order: int
    step_ids: List[str]


class LabelGraphNode(BaseModel):
    id: str
    label: str
    status: str
    kind: Literal["label", "container", "interleaved-group"]
    step_ids: List[str]
    step_count: int
    parent_id: Optional[str] = None
    children_ids: List[str] = Field(default_factory=list)
    order_min: int
    order_max: int
    segments: List[LabelSegment] = Field(default_factory=list)


class LabelGraphEdge(BaseModel):
    id: str
    source: str
    target: str
    kind: Literal["order", "exact-order", "secondary"]
    visible_by_default: bool
    show_when_selected: Optional[List[str]] = None
    replaces_edge_id: Optional[str] = None
    source_scope: Optional[Literal["node", "container", "child"]] = None
    target_scope: Optional[Literal["node", "container", "child"]] = None


class LabelContainment(BaseModel):
    parent: str
    child: str
    kind: Literal["semantic", "explicit", "heuristic"]


class LabelSharedRelation(BaseModel):
    id: str
    a: str
    b: str
    shared_step_ids: List[str]
    shared_count: int
    visible_by_default: bool = False


class LabelInterleaving(BaseModel):
    id: str
    labels: List[str]
    segments: List[LabelSegment]
    switch_count: int
    visible_by_default: bool = True


class LabelGraphPayload(BaseModel):
    label_key: str
    nodes: List[LabelGraphNode]
    edges: List[LabelGraphEdge]
    containments: List[LabelContainment] = Field(default_factory=list)
    shared_relations: List[LabelSharedRelation] = Field(default_factory=list)
    interleavings: List[LabelInterleaving] = Field(default_factory=list)


class StageStepStatus(BaseModel):
    name: str
    total_idx_count: int = 0
    changed_idx_count: int = 0
    has_backlog: bool = False


class StageItem(BaseModel):
    stage: str
    status: str
    steps: List[StageStepStatus] = Field(default_factory=list)


class StageEdge(BaseModel):
    from_: str = Field(alias="from")
    to: str
    count: Optional[int] = None

    model_config = {"populate_by_name": True}


class PipelineDetailResponse(BaseModel):
    """Typed pipeline overview for General / Graph label chrome (no runs store)."""

    stages: List[StageItem] = Field(default_factory=list)
    stage_edges: List[StageEdge] = Field(default_factory=list)
    label_graph: Optional[LabelGraphPayload] = None
    available_label_keys: List[str] = Field(default_factory=list)
