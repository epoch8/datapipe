import asyncio
import uuid
import logging

from typing import Tuple
from dataclasses import dataclass, field

from datapipe_router.run.store.base import BaseEventStore
from datapipe_router.pb2.storage_client_pb2 import LogEvent, StatusEvent


logger = logging.getLogger(__name__)


@dataclass
class RunSubscribers:
    log_subscribers: dict[str, asyncio.Queue] = field(default_factory=dict)
    status_subscribers: dict[str,  asyncio.Queue] = field(default_factory=dict)


class RunEventManager:
    def __init__(self, store: BaseEventStore):
        self.store = store
        self.runs: dict[str, RunSubscribers] = {}

    async def set_status(self, run_id:str, event: StatusEvent):
        await self.store.add_status_event(run_id, event)

        if run_id in self.runs:
            for queue in self.runs[run_id].status_subscribers.values():
                await queue.put((event.timestamp, event))
    
    async def add_log_record(self, run_id:str, event: LogEvent):
        await self.store.add_log_event(run_id, event)
        
        if run_id in self.runs:
            for queue in self.runs[run_id].log_subscribers.values():
                await queue.put((event.sequence, event))

    async def get_logs(self, run_id: str) -> list[LogEvent]:
        return await self.store.get_logs(run_id)

    async def get_statuses(self, run_id: str) -> list[StatusEvent]:
        return await self.store.get_statuses(run_id)

    async def subscribe_log(self, run_id: str) -> Tuple[str, asyncio.PriorityQueue]:
        subscribe_id = str(uuid.uuid4())
        queue = asyncio.PriorityQueue()

        if run_id not in self.runs:
            self.runs[run_id] = RunSubscribers()

        self.runs[run_id].log_subscribers[subscribe_id] = queue

        async for event in self.get_logs(run_id):
            await queue.put((event.sequence, event))

        return subscribe_id, queue

    async def subscribe_status(self, run_id) -> Tuple[str, asyncio.PriorityQueue]:
        subscribe_id = str(uuid.uuid4())
        queue = asyncio.PriorityQueue()

        if run_id not in self.runs:
            self.runs[run_id] = RunSubscribers()

        self.runs[run_id].status_subscribers[subscribe_id] = queue

        async for event in self.get_logs(run_id):
            await queue.put((event.timestamp, event))

        return subscribe_id, queue
    
    async def unsubscribe_log(self, run_id:str, subscribe_id: str):
        if run_id in self.runs:
            run = self.runs[run_id]

            if subscribe_id in run.log_subscribers:
                await run.log_subscribers[subscribe_id].put(None)
                del run.log_subscribers[subscribe_id]

            if not run.log_subscribers and not run.status_subscribers:
                del self.runs[run_id]

    async def unsubscribe_status(self, run_id:str, subscribe_id: str):
        if run_id in self.runs:
            run = self.runs[run_id]

            if subscribe_id in run.status_subscribers:
                await run.status_subscribers[subscribe_id].put(None)
                del run.status_subscribers[subscribe_id]

            if not run.log_subscribers and not run.status_subscribers:
                del self.runs[run_id]

    async def unsubscribe_all(self, run_id: str):
        if run_id in self.runs:
            run = self.runs[run_id]

            for queue in run.log_subscribers.values():
                await queue.put(None)

            for queue in run.status_subscribers.values():
                await queue.put(None)

            del self.runs[run_id]