from datapipe_router.pb2.storage_client_pb2 import LogEvent, StatusEvent
from datapipe_router.run.store.base import BaseEventStore

class MemoryEventStore(BaseEventStore):

    def __init__(self):
        self.logs: dict[str, list[LogEvent]] = {}
        self.statuses: dict[str, list[StatusEvent]] = {} 

    async def get_logs(self, run_id: str) -> list[LogEvent]: 
        if run_id not in self.logs:
            return None

        return self.logs[run_id]

    async def get_statuses(self, run_id: str) -> list[StatusEvent]: 
        if run_id not in self.statuses:
            return None

        return self.statuses[run_id]

    async def add_status_event(self, run_id: str, event: StatusEvent):
        if run_id not in self.statuses:
            self.statuses[run_id] = []
        print(event)
        return self.statuses[run_id].append(event)
            
    async def add_log_event(self, run_id: str, event: LogEvent):
        if run_id not in self.logs:
            self.logs[run_id] = []
        print(event)
        return self.logs[run_id].append(event)