import storage_client_pb2 as _storage_client_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class SendLogsRequest(_message.Message):
    __slots__ = ("run_id", "log")
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    LOG_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    log: _storage_client_pb2.LogEvent
    def __init__(self, run_id: _Optional[str] = ..., log: _Optional[_Union[_storage_client_pb2.LogEvent, _Mapping]] = ...) -> None: ...

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
    status: _storage_client_pb2.StatusEvent
    def __init__(self, run_id: _Optional[str] = ..., status: _Optional[_Union[_storage_client_pb2.StatusEvent, _Mapping]] = ...) -> None: ...

class SendRunStatusResponse(_message.Message):
    __slots__ = ("state",)
    STATE_FIELD_NUMBER: _ClassVar[int]
    state: str
    def __init__(self, state: _Optional[str] = ...) -> None: ...
