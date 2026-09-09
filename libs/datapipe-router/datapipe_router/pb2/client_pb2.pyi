from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class GetDataRequest(_message.Message):
    __slots__ = ("agent_id", "table", "page", "page_size", "order", "include_total", "order_by", "filters", "focus")
    AGENT_ID_FIELD_NUMBER: _ClassVar[int]
    TABLE_FIELD_NUMBER: _ClassVar[int]
    PAGE_FIELD_NUMBER: _ClassVar[int]
    PAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    ORDER_FIELD_NUMBER: _ClassVar[int]
    INCLUDE_TOTAL_FIELD_NUMBER: _ClassVar[int]
    ORDER_BY_FIELD_NUMBER: _ClassVar[int]
    FILTERS_FIELD_NUMBER: _ClassVar[int]
    FOCUS_FIELD_NUMBER: _ClassVar[int]
    agent_id: str
    table: str
    page: int
    page_size: int
    order: str
    include_total: bool
    order_by: str
    filters: bytes
    focus: _containers.RepeatedScalarFieldContainer[bytes]
    def __init__(self, agent_id: _Optional[str] = ..., table: _Optional[str] = ..., page: _Optional[int] = ..., page_size: _Optional[int] = ..., order: _Optional[str] = ..., include_total: bool = ..., order_by: _Optional[str] = ..., filters: _Optional[bytes] = ..., focus: _Optional[_Iterable[bytes]] = ...) -> None: ...

class GetDataResponse(_message.Message):
    __slots__ = ("page", "page_size", "data", "total")
    PAGE_FIELD_NUMBER: _ClassVar[int]
    PAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    DATA_FIELD_NUMBER: _ClassVar[int]
    TOTAL_FIELD_NUMBER: _ClassVar[int]
    page: int
    page_size: int
    data: bytes
    total: int
    def __init__(self, page: _Optional[int] = ..., page_size: _Optional[int] = ..., data: _Optional[bytes] = ..., total: _Optional[int] = ...) -> None: ...

class GetAgentsRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class GetAgentsResponse(_message.Message):
    __slots__ = ("agents",)
    AGENTS_FIELD_NUMBER: _ClassVar[int]
    agents: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, agents: _Optional[_Iterable[str]] = ...) -> None: ...

class RunPipelineRequest(_message.Message):
    __slots__ = ("agent_id",)
    AGENT_ID_FIELD_NUMBER: _ClassVar[int]
    agent_id: str
    def __init__(self, agent_id: _Optional[str] = ...) -> None: ...

class RunPipelineResponse(_message.Message):
    __slots__ = ("run_id",)
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    def __init__(self, run_id: _Optional[str] = ...) -> None: ...

class GetRunListRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class RunInfo(_message.Message):
    __slots__ = ("run_id", "agent_id", "status")
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    AGENT_ID_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    agent_id: str
    status: str
    def __init__(self, run_id: _Optional[str] = ..., agent_id: _Optional[str] = ..., status: _Optional[str] = ...) -> None: ...

class GetRunListResponse(_message.Message):
    __slots__ = ("runs",)
    RUNS_FIELD_NUMBER: _ClassVar[int]
    runs: _containers.RepeatedCompositeFieldContainer[RunInfo]
    def __init__(self, runs: _Optional[_Iterable[_Union[RunInfo, _Mapping]]] = ...) -> None: ...

class GetRunLogsRequest(_message.Message):
    __slots__ = ("run_id",)
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    def __init__(self, run_id: _Optional[str] = ...) -> None: ...

class GetRunLogsResponse(_message.Message):
    __slots__ = ("logs",)
    LOGS_FIELD_NUMBER: _ClassVar[int]
    logs: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, logs: _Optional[_Iterable[str]] = ...) -> None: ...

class GetRunLogsStreamRequest(_message.Message):
    __slots__ = ("run_id",)
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    def __init__(self, run_id: _Optional[str] = ...) -> None: ...

class GetRunLogsStreamResponse(_message.Message):
    __slots__ = ("logs",)
    LOGS_FIELD_NUMBER: _ClassVar[int]
    logs: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, logs: _Optional[_Iterable[str]] = ...) -> None: ...

class GetGraphRequest(_message.Message):
    __slots__ = ("agent_id", "label_key", "value")
    AGENT_ID_FIELD_NUMBER: _ClassVar[int]
    LABEL_KEY_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    agent_id: str
    label_key: str
    value: str
    def __init__(self, agent_id: _Optional[str] = ..., label_key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...

class TableColumn(_message.Message):
    __slots__ = ("name", "type")
    NAME_FIELD_NUMBER: _ClassVar[int]
    TYPE_FIELD_NUMBER: _ClassVar[int]
    name: str
    type: str
    def __init__(self, name: _Optional[str] = ..., type: _Optional[str] = ...) -> None: ...

class TableDetails(_message.Message):
    __slots__ = ("name", "store_class", "indexes", "schema", "size")
    NAME_FIELD_NUMBER: _ClassVar[int]
    STORE_CLASS_FIELD_NUMBER: _ClassVar[int]
    INDEXES_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_FIELD_NUMBER: _ClassVar[int]
    SIZE_FIELD_NUMBER: _ClassVar[int]
    name: str
    store_class: str
    indexes: _containers.RepeatedScalarFieldContainer[str]
    schema: _containers.RepeatedCompositeFieldContainer[TableColumn]
    size: int
    def __init__(self, name: _Optional[str] = ..., store_class: _Optional[str] = ..., indexes: _Optional[_Iterable[str]] = ..., schema: _Optional[_Iterable[_Union[TableColumn, _Mapping]]] = ..., size: _Optional[int] = ...) -> None: ...

class LabelsItem(_message.Message):
    __slots__ = ("item",)
    ITEM_FIELD_NUMBER: _ClassVar[int]
    item: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, item: _Optional[_Iterable[str]] = ...) -> None: ...

class PipelineStepDetail(_message.Message):
    __slots__ = ("name", "type", "transform_type", "inputs", "outputs", "labels", "has_transform_meta", "indexes", "total_idx_count", "changed_idx_count")
    NAME_FIELD_NUMBER: _ClassVar[int]
    TYPE_FIELD_NUMBER: _ClassVar[int]
    TRANSFORM_TYPE_FIELD_NUMBER: _ClassVar[int]
    INPUTS_FIELD_NUMBER: _ClassVar[int]
    OUTPUTS_FIELD_NUMBER: _ClassVar[int]
    LABELS_FIELD_NUMBER: _ClassVar[int]
    HAS_TRANSFORM_META_FIELD_NUMBER: _ClassVar[int]
    INDEXES_FIELD_NUMBER: _ClassVar[int]
    TOTAL_IDX_COUNT_FIELD_NUMBER: _ClassVar[int]
    CHANGED_IDX_COUNT_FIELD_NUMBER: _ClassVar[int]
    name: str
    type: str
    transform_type: str
    inputs: _containers.RepeatedScalarFieldContainer[str]
    outputs: _containers.RepeatedScalarFieldContainer[str]
    labels: _containers.RepeatedCompositeFieldContainer[LabelsItem]
    has_transform_meta: bool
    indexes: _containers.RepeatedScalarFieldContainer[str]
    total_idx_count: int
    changed_idx_count: int
    def __init__(self, name: _Optional[str] = ..., type: _Optional[str] = ..., transform_type: _Optional[str] = ..., inputs: _Optional[_Iterable[str]] = ..., outputs: _Optional[_Iterable[str]] = ..., labels: _Optional[_Iterable[_Union[LabelsItem, _Mapping]]] = ..., has_transform_meta: bool = ..., indexes: _Optional[_Iterable[str]] = ..., total_idx_count: _Optional[int] = ..., changed_idx_count: _Optional[int] = ...) -> None: ...

class MetaStepDetail(_message.Message):
    __slots__ = ("name", "type", "transform_type", "inputs", "outputs", "labels", "graph")
    NAME_FIELD_NUMBER: _ClassVar[int]
    TYPE_FIELD_NUMBER: _ClassVar[int]
    TRANSFORM_TYPE_FIELD_NUMBER: _ClassVar[int]
    INPUTS_FIELD_NUMBER: _ClassVar[int]
    OUTPUTS_FIELD_NUMBER: _ClassVar[int]
    LABELS_FIELD_NUMBER: _ClassVar[int]
    GRAPH_FIELD_NUMBER: _ClassVar[int]
    name: str
    type: str
    transform_type: str
    inputs: _containers.RepeatedScalarFieldContainer[str]
    outputs: _containers.RepeatedScalarFieldContainer[str]
    labels: _containers.RepeatedCompositeFieldContainer[LabelsItem]
    graph: GetGraphResponse
    def __init__(self, name: _Optional[str] = ..., type: _Optional[str] = ..., transform_type: _Optional[str] = ..., inputs: _Optional[_Iterable[str]] = ..., outputs: _Optional[_Iterable[str]] = ..., labels: _Optional[_Iterable[_Union[LabelsItem, _Mapping]]] = ..., graph: _Optional[_Union[GetGraphResponse, _Mapping]] = ...) -> None: ...

class PipelineNodeDetails(_message.Message):
    __slots__ = ("pipeline_step", "meta_step")
    PIPELINE_STEP_FIELD_NUMBER: _ClassVar[int]
    META_STEP_FIELD_NUMBER: _ClassVar[int]
    pipeline_step: PipelineStepDetail
    meta_step: MetaStepDetail
    def __init__(self, pipeline_step: _Optional[_Union[PipelineStepDetail, _Mapping]] = ..., meta_step: _Optional[_Union[MetaStepDetail, _Mapping]] = ...) -> None: ...

class GetGraphResponse(_message.Message):
    __slots__ = ("catalog", "pipeline", "stages")
    class CatalogEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: TableDetails
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[TableDetails, _Mapping]] = ...) -> None: ...
    CATALOG_FIELD_NUMBER: _ClassVar[int]
    PIPELINE_FIELD_NUMBER: _ClassVar[int]
    STAGES_FIELD_NUMBER: _ClassVar[int]
    catalog: _containers.MessageMap[str, TableDetails]
    pipeline: _containers.RepeatedCompositeFieldContainer[PipelineNodeDetails]
    stages: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, catalog: _Optional[_Mapping[str, TableDetails]] = ..., pipeline: _Optional[_Iterable[_Union[PipelineNodeDetails, _Mapping]]] = ..., stages: _Optional[_Iterable[str]] = ...) -> None: ...
