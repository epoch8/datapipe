from datapipe_router.pb2.storage_client_pb2 import LogEvent, StatusEvent


class BaseEventStore:

    async def get_logs(self, run_id: str) -> list[LogEvent]: 
        raise NotImplementedError()

    async def get_statuses(self, run_id: str) -> list[StatusEvent]: 
        raise NotImplementedError()

    async def add_status_event(self, run_id: str, event: StatusEvent):
        raise NotImplementedError()
            
    async def add_log_event(self, run_id: str, event: LogEvent):
        raise NotImplementedError()