import client_pb2 as _client_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
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
    request: _client_pb2.GetDataRequest
    def __init__(self, route_id: _Optional[str] = ..., request: _Optional[_Union[_client_pb2.GetDataRequest, _Mapping]] = ...) -> None: ...

class RunEvent(_message.Message):
    __slots__ = ("run_id",)
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    def __init__(self, run_id: _Optional[str] = ...) -> None: ...

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
    data: _client_pb2.GetDataResponse
    def __init__(self, route_id: _Optional[str] = ..., data: _Optional[_Union[_client_pb2.GetDataResponse, _Mapping]] = ...) -> None: ...

class SendDataResponse(_message.Message):
    __slots__ = ("status",)
    STATUS_FIELD_NUMBER: _ClassVar[int]
    status: str
    def __init__(self, status: _Optional[str] = ...) -> None: ...

class SendLogsRequest(_message.Message):
    __slots__ = ("run_id", "log")
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    LOG_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    log: str
    def __init__(self, run_id: _Optional[str] = ..., log: _Optional[str] = ...) -> None: ...

class SendLogsResponse(_message.Message):
    __slots__ = ("state",)
    STATE_FIELD_NUMBER: _ClassVar[int]
    state: str
    def __init__(self, state: _Optional[str] = ...) -> None: ...

class SendRunStatusRequest(_message.Message):
    __slots__ = ("run_id", "status")
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    status: str
    def __init__(self, run_id: _Optional[str] = ..., status: _Optional[str] = ...) -> None: ...

class SendRunStatusResponse(_message.Message):
    __slots__ = ("state",)
    STATE_FIELD_NUMBER: _ClassVar[int]
    state: str
    def __init__(self, state: _Optional[str] = ...) -> None: ...

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
    data: _client_pb2.GetGraphResponse
    def __init__(self, route_id: _Optional[str] = ..., data: _Optional[_Union[_client_pb2.GetGraphResponse, _Mapping]] = ...) -> None: ...

class SendGraphResponse(_message.Message):
    __slots__ = ("status",)
    STATUS_FIELD_NUMBER: _ClassVar[int]
    status: str
    def __init__(self, status: _Optional[str] = ...) -> None: ...
