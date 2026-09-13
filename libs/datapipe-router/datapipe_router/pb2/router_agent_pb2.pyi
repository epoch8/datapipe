import router_client_pb2 as _router_client_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class ServerEventsRequest(_message.Message):
    __slots__ = ("name",)
    NAME_FIELD_NUMBER: _ClassVar[int]
    name: str
    def __init__(self, name: _Optional[str] = ...) -> None: ...

class DataEvent(_message.Message):
    __slots__ = ("route_id", "request")
    ROUTE_ID_FIELD_NUMBER: _ClassVar[int]
    REQUEST_FIELD_NUMBER: _ClassVar[int]
    route_id: str
    request: _router_client_pb2.GetDataRequest
    def __init__(self, route_id: _Optional[str] = ..., request: _Optional[_Union[_router_client_pb2.GetDataRequest, _Mapping]] = ...) -> None: ...

class RunEvent(_message.Message):
    __slots__ = ("route_id", "run_id", "labels", "changelist")
    ROUTE_ID_FIELD_NUMBER: _ClassVar[int]
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    LABELS_FIELD_NUMBER: _ClassVar[int]
    CHANGELIST_FIELD_NUMBER: _ClassVar[int]
    route_id: str
    run_id: str
    labels: _containers.RepeatedCompositeFieldContainer[_router_client_pb2.LabelsItem]
    changelist: _containers.RepeatedCompositeFieldContainer[_router_client_pb2.ChangeList]
    def __init__(self, route_id: _Optional[str] = ..., run_id: _Optional[str] = ..., labels: _Optional[_Iterable[_Union[_router_client_pb2.LabelsItem, _Mapping]]] = ..., changelist: _Optional[_Iterable[_Union[_router_client_pb2.ChangeList, _Mapping]]] = ...) -> None: ...

class PingEvent(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class GraphEvent(_message.Message):
    __slots__ = ("route_id", "label_key", "value")
    ROUTE_ID_FIELD_NUMBER: _ClassVar[int]
    LABEL_KEY_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    route_id: str
    label_key: str
    value: str
    def __init__(self, route_id: _Optional[str] = ..., label_key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...

class ServerEventsResponse(_message.Message):
    __slots__ = ("request_id", "data_event", "run_event", "graph_event", "ping_event")
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    DATA_EVENT_FIELD_NUMBER: _ClassVar[int]
    RUN_EVENT_FIELD_NUMBER: _ClassVar[int]
    GRAPH_EVENT_FIELD_NUMBER: _ClassVar[int]
    PING_EVENT_FIELD_NUMBER: _ClassVar[int]
    request_id: str
    data_event: DataEvent
    run_event: RunEvent
    graph_event: GraphEvent
    ping_event: PingEvent
    def __init__(self, request_id: _Optional[str] = ..., data_event: _Optional[_Union[DataEvent, _Mapping]] = ..., run_event: _Optional[_Union[RunEvent, _Mapping]] = ..., graph_event: _Optional[_Union[GraphEvent, _Mapping]] = ..., ping_event: _Optional[_Union[PingEvent, _Mapping]] = ...) -> None: ...

class SendDataRequest(_message.Message):
    __slots__ = ("route_id", "data")
    ROUTE_ID_FIELD_NUMBER: _ClassVar[int]
    DATA_FIELD_NUMBER: _ClassVar[int]
    route_id: str
    data: _router_client_pb2.TableData
    def __init__(self, route_id: _Optional[str] = ..., data: _Optional[_Union[_router_client_pb2.TableData, _Mapping]] = ...) -> None: ...

class SendDataResponse(_message.Message):
    __slots__ = ("status",)
    STATUS_FIELD_NUMBER: _ClassVar[int]
    status: str
    def __init__(self, status: _Optional[str] = ...) -> None: ...

class PingRequest(_message.Message):
    __slots__ = ("name",)
    NAME_FIELD_NUMBER: _ClassVar[int]
    name: str
    def __init__(self, name: _Optional[str] = ...) -> None: ...

class PingResponse(_message.Message):
    __slots__ = ("status", "message")
    STATUS_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    status: str
    message: str
    def __init__(self, status: _Optional[str] = ..., message: _Optional[str] = ...) -> None: ...

class SendGraphRequest(_message.Message):
    __slots__ = ("route_id", "data")
    ROUTE_ID_FIELD_NUMBER: _ClassVar[int]
    DATA_FIELD_NUMBER: _ClassVar[int]
    route_id: str
    data: _router_client_pb2.GraphData
    def __init__(self, route_id: _Optional[str] = ..., data: _Optional[_Union[_router_client_pb2.GraphData, _Mapping]] = ...) -> None: ...

class SendGraphResponse(_message.Message):
    __slots__ = ("status",)
    STATUS_FIELD_NUMBER: _ClassVar[int]
    status: str
    def __init__(self, status: _Optional[str] = ...) -> None: ...

class SendRunCreationStatusRequest(_message.Message):
    __slots__ = ("route_id", "state", "error")
    ROUTE_ID_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    route_id: str
    state: str
    error: str
    def __init__(self, route_id: _Optional[str] = ..., state: _Optional[str] = ..., error: _Optional[str] = ...) -> None: ...

class SendRunCreationStatusResponse(_message.Message):
    __slots__ = ("state",)
    STATE_FIELD_NUMBER: _ClassVar[int]
    state: str
    def __init__(self, state: _Optional[str] = ...) -> None: ...
