import logging

from datapipe_router.pb2.storage_agent_pb2_grpc import StorageDatapipeServiceServicer
from datapipe_router.pb2.storage_client_pb2_grpc import StorageClientServiceServicer
from datapipe_router.pb2.storage_agent_pb2 import SendLogsResponse, SendRunStatusResponse
from datapipe_router.pb2.storage_client_pb2 import (
    GetRunLogsResponse, 
    GetRunStatusesResponse, 
    GetRunLogsStreamResponse,
    GetRunStatusesStreamResponse,
)

from datapipe_router.datastore import ServerDataStore
from datapipe_router.run.manager import RunEventManager
from datapipe_router.run.store.base import BaseEventStore
from datapipe_router.run.store.memory import MemoryEventStore


STREAM_PING_DELAY = 10
GED_DATA_TIMEOUT = 10


logger = logging.getLogger(__name__)


class StorageServicer(
    StorageDatapipeServiceServicer,
    StorageClientServiceServicer,
):
    def __init__(self, store: ServerDataStore, event_store: BaseEventStore = None):
        if not event_store:
            event_store = MemoryEventStore()

        self.run_manager = RunEventManager(store=event_store) 
        self.store = store

    async def SendLogs(self, request, context):
        await self.run_manager.add_log_record(request.run_id, request.log)

        return SendLogsResponse(state="ok")
    
    async def SendRunStatus(self, request, context):
        await self.run_manager.set_status(request.run_id, request.status)
        
        return SendRunStatusResponse(state="ok")

    async def GetRunLogs(self, request, context):
        logs = await self.run_manager.get_logs(request.run_id)

        raise GetRunLogsResponse(logs=logs)

    async def GetRunStatuses(self, request, context):
        statuses = await self.run_manager.get_statuses(request.run_id)
        
        raise GetRunStatusesResponse(statuses=statuses)

    async def GetRunLogsStream(self, request, context):
        subscribe_id, queue = await self.run_manager.subscribe_logs(request.run_id)

        try:
            async for _, event in self.read_stream(queue):
                if event is None:
                    break

                yield GetRunLogsStreamResponse(log=event)
        finally:
            await self.run_manager.unsubscribe_logs(request.run_id, subscribe_id)


    async def GetRunStatusesStream(self, request, context):
        subscribe_id, queue = await self.run_manager.subscribe_statuses(request.run_id)

        try:
            async for _, event in self.read_stream(queue):
                if event is None:
                    break

                yield GetRunStatusesStreamResponse(status=event)
        finally:
            await self.run_manager.unsubscribe_statuses(request.run_id, subscribe_id)


    async def read_stream(self, queue):
        while(True):
            item = await queue.get()

            if item is None:
                break

            yield item 
    