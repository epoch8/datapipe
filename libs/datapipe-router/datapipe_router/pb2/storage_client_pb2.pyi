from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class LogEvent(_message.Message):
    __slots__ = ("timestamp", "sequence", "log")
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    SEQUENCE_FIELD_NUMBER: _ClassVar[int]
    LOG_FIELD_NUMBER: _ClassVar[int]
    timestamp: float
    sequence: int
    log: str
    def __init__(self, timestamp: _Optional[float] = ..., sequence: _Optional[int] = ..., log: _Optional[str] = ...) -> None: ...

class StatusEvent(_message.Message):
    __slots__ = ("timestamp", "status")
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    timestamp: float
    status: str
    def __init__(self, timestamp: _Optional[float] = ..., status: _Optional[str] = ...) -> None: ...

class GetRunLogsRequest(_message.Message):
    __slots__ = ("run_id",)
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    def __init__(self, run_id: _Optional[str] = ...) -> None: ...

class GetRunLogsResponse(_message.Message):
    __slots__ = ("logs",)
    LOGS_FIELD_NUMBER: _ClassVar[int]
    logs: _containers.RepeatedCompositeFieldContainer[LogEvent]
    def __init__(self, logs: _Optional[_Iterable[_Union[LogEvent, _Mapping]]] = ...) -> None: ...

class GetRunStatusesRequest(_message.Message):
    __slots__ = ("run_id",)
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    def __init__(self, run_id: _Optional[str] = ...) -> None: ...

class GetRunStatusesResponse(_message.Message):
    __slots__ = ("statuses",)
    STATUSES_FIELD_NUMBER: _ClassVar[int]
    statuses: _containers.RepeatedCompositeFieldContainer[StatusEvent]
    def __init__(self, statuses: _Optional[_Iterable[_Union[StatusEvent, _Mapping]]] = ...) -> None: ...

class GetRunLogsStreamRequest(_message.Message):
    __slots__ = ("run_id",)
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    def __init__(self, run_id: _Optional[str] = ...) -> None: ...

class GetRunLogsStreamResponse(_message.Message):
    __slots__ = ("log",)
    LOG_FIELD_NUMBER: _ClassVar[int]
    log: LogEvent
    def __init__(self, log: _Optional[_Union[LogEvent, _Mapping]] = ...) -> None: ...

class GetRunStatusesStreamRequest(_message.Message):
    __slots__ = ("run_id",)
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    def __init__(self, run_id: _Optional[str] = ...) -> None: ...

class GetRunStatusesStreamResponse(_message.Message):
    __slots__ = ("status",)
    STATUS_FIELD_NUMBER: _ClassVar[int]
    status: StatusEvent
    def __init__(self, status: _Optional[_Union[StatusEvent, _Mapping]] = ...) -> None: ...
