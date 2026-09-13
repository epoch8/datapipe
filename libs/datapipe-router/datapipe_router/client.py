import grpc

from typing import AsyncGenerator, Any, List, Optional, Literal

from datapipe_router.pb2.router_client_pb2_grpc import RouterClientServiceStub
from datapipe_router.pb2.storage_client_pb2_grpc import StorageClientServiceStub
from datapipe_router.pb2.router_client_pb2 import (
    LabelsItem,
    GetAgentsRequest,
    GetDataRequest,
    GetGraphRequest,
    GetGraphResponse,
    RunPipelineRequest,
    CancelPipelineRequest,
)
from datapipe_router.pb2.storage_client_pb2 import (
    GetRunLogsRequest,
    GetRunStatusesRequest,
    GetRunLogsStreamRequest,
    GetRunStatusesStreamRequest,
)


from datapipe_router.types import (
    Graph, 
    DataFilter, 
    TableData, 
    ChangeList, 
    Event, 
    LogEvent, 
    StatusEvent
)


CHANNEL_OPTIONS = [
    ('grpc.keepalive_time_ms', 30000),               # Send pings every 30 seconds if idle
    ('grpc.keepalive_timeout_ms', 10000),            # Wait 10 seconds for ping response
    ('grpc.keepalive_permit_without_calls', 1),      # Allow pings even if there are no active streams
    ('grpc.http2.max_pings_without_data', 0),        # Unlimited pings without sending data
    ("grpc.http2.min_time_between_pings_ms", 10000), # Enforce minimum time between pings to remain gentle on the proxy
]


class RouterClient:

    def __init__(self, host:str = None, port:int = 10500):
        self.host = host
        self.port = port

        self._channel = None
        self._stub = None

        self.runs = {}

    def _get_grpc_stub(self):
        if self._channel is None:
            # Created once and reused across the entire application lifetime
            url = f'{self.host}:{self.port}'
            self._channel = grpc.aio.insecure_channel(url, options=CHANNEL_OPTIONS)
            self._stub = RouterClientServiceStub(self._channel)

        return self._stub

    async def get_agents(self) -> list[str]:
        stub = self._get_grpc_stub()
        response = await stub.GetAgents(GetAgentsRequest())

        print(f"Client received: {response.agents}")

        return list(response.agents)

    async def get_data(
        self, 
        agent_id: str, 
        table: str,
        page: int = 0,
        page_size: int = 5,
        include_total: bool = False,
        order: Literal["asc", "desc"] = "asc",
        order_by: Optional[str] = None,
        filters: Optional[DataFilter] = None,
        focus: Optional[List[DataFilter]] = [],
    ) -> TableData:
        request = GetDataRequest(
            agent_id=agent_id,
            table=table,
            page=page,
            page_size=page_size,
            include_total=include_total,
            order=order,
            order_by=order_by,
            filters=filters.to_bytes() if filters else None,
            focus=[item.to_bytes() for item in focus]
        )

        stub = self._get_grpc_stub()
        response = await stub.GetData(request)

        if not response.data:
            return None

        return TableData.from_message(response.data)

    async def get_graph(self, agent_id: str, label_key: str, value: str = None)  -> Graph:
        request = GetGraphRequest(
            agent_id=agent_id,
            label_key=label_key,
            value=value
        )

        stub = self._get_grpc_stub()
        response = await stub.GetGraph(request)

        if response == GetGraphResponse():
            return None

        return Graph.from_message(response.data)

    async def run_pipeline(
        self, 
        agent_id: str, 
        run_id: str,
        labels: List[tuple[str, str]] = [], 
        changelist: List[ChangeList] = []
    ) -> str:
        request = RunPipelineRequest(
            agent_id=agent_id,
            run_id=run_id,
            labels=[LabelsItem(item=label) for label in labels],
            changelist=[item.to_message() for item in changelist]
        )
        stub = self._get_grpc_stub()
        response = await stub.RunPipeline(request)

        return response.status

    async def cancel_pipeline(
        self, 
        agent_id: str, 
        run_id: str,
    ) -> str:
        request = CancelPipelineRequest(
            agent_id=agent_id,
            run_id=run_id,
        )
        stub = self._get_grpc_stub()
        response = await stub.CancelPipeline(request)

        return response.status 


class StorageClient:

    def __init__(self, host:str = None, port:int = 10500):
        self.host = host
        self.port = port

        self._channel = None
        self._stub = None

        self.runs = {}

    def _get_grpc_stub(self):
        if self._channel is None:
            # Created once and reused across the entire application lifetime
            url = f'{self.host}:{self.port}'
            self._channel = grpc.aio.insecure_channel(url, options=CHANNEL_OPTIONS)
            self._stub = StorageClientServiceStub(self._channel)

        return self._stub

    async def get_run_logs(self, run_id: str) -> list[LogEvent]:
        request = GetRunLogsRequest(run_id=run_id)
        stub = self._get_grpc_stub()
        response = await stub.GetRunLogs(request)

        return [
            LogEvent(timestamp=event.timestamp, sequence=event.sequence, log=event.log)
            for event in response.logs
        ]

    async def get_run_statuses(self, run_id: str) -> list[StatusEvent]:
        request = GetRunStatusesRequest(run_id=run_id)
        stub = self._get_grpc_stub()
        response = await stub.GetRunStatuses(request)

        return [
            StatusEvent(timestamp=event.timestamp, status=event.status)
            for event in response.statuses
        ]

    async def get_run_logs_stream(self, run_id: str) -> AsyncGenerator[list[LogEvent], Any]:
        try:
            stub = self._get_grpc_stub()
            request = GetRunLogsStreamRequest(run_id=run_id)
            
            response_stream = stub.GetRunEventsStream(request)
            
            async for event in response_stream:
                yield LogEvent(
                    timestamp=event.timestamp, 
                    sequence=event.sequence, 
                    log=event.log
                )

        except grpc.aio.AioRpcError as e:
            print(e)
            if e.code() == grpc.StatusCode.CANCELLED:
                return

            elif e.code() == grpc.StatusCode.UNAVAILABLE:
                return

            else:
                raise e

    async def get_run_statuses_stream(self, run_id: str) -> AsyncGenerator[list[StatusEvent], Any]:
        try:
            stub = self._get_grpc_stub()
            request = GetRunStatusesStreamRequest(run_id=run_id)
            
            response_stream = stub.GetRunEventsStream(request)
            
            async for event in response_stream:
                yield StatusEvent(
                    timestamp=event.timestamp, 
                    status=event.status
                )

        except grpc.aio.AioRpcError as e:
            print(e)
            if e.code() == grpc.StatusCode.CANCELLED:
                return

            elif e.code() == grpc.StatusCode.UNAVAILABLE:
                return

            else:
                raise e