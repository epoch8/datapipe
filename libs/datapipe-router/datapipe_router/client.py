import grpc
import io
import pandas as pd

from typing import AsyncGenerator, Any, List, Dict, Optional, Literal
from dataclasses import dataclass


import datapipe_router.pb2.client_pb2 as client_messages
import datapipe_router.pb2.client_pb2_grpc as client_messages_grpc

from datapipe_router.types import Graph, DataFilter, TableData


CHANNEL_OPTIONS = [
    ('grpc.keepalive_time_ms', 30000),             # Send pings every 30 seconds if idle
    ('grpc.keepalive_timeout_ms', 10000),          # Wait 10 seconds for ping response
    ('grpc.keepalive_permit_without_calls', 1),    # Allow pings even if there are no active streams
    ('grpc.http2.max_pings_without_data', 0),      # Unlimited pings without sending data
]


class DatapipeClient:

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
            self._stub = client_messages_grpc.ClientServiceStub(self._channel)

        return self._stub


    async def get_agents(self) -> list[str]:
        stub = self._get_grpc_stub()
        response = await stub.GetAgents(client_messages.GetAgentsRequest())

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
        request = client_messages.GetDataRequest(
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

        return TableData.from_message(response)

    async def get_graph(self, agent_id: str, label_key: str, value: str = None)  -> Graph:
        request = client_messages.GetGraphRequest(
            agent_id=agent_id,
            label_key=label_key,
            value=value
        )

        stub = self._get_grpc_stub()
        response = await stub.GetGraph(request)

        if response == client_messages.GetGraphResponse():
            return None

        return Graph.from_message(response)


    async def get_runs(self) -> list[dict[str, str]]:
        request = client_messages.GetRunListRequest()
        stub = self._get_grpc_stub()
        response = await stub.GetRunList(request)

        return [
            {
                "run_id": run.run_id,
                "agent_id": run.agent_id,
                "status": run.status
            }
            for run in response.runs
        ]

    async def get_run_logs(self, run_id: str) -> list[str]:
        request = client_messages.GetRunLogsRequest(run_id=run_id)
        stub = self._get_grpc_stub()
        response = await stub.GetRunLogs(request)

        return list(response.logs)


    async def run_pipeline(self, agent_id: str) -> str:
        request = client_messages.RunPipelineRequest(agent_id=agent_id)
        stub = self._get_grpc_stub()
        response = await stub.RunPipeline(request)

        return response.run_id


    async def get_run_logs_stream(self, run_id: str) -> AsyncGenerator[list[str], Any]:
        try:
            stub = self._get_grpc_stub()
            request = client_messages.GetRunLogsStreamRequest(run_id=run_id)
            
            response_stream = stub.GetRunLogsStream(request)
            print(run_id)
            async for event in response_stream:
                yield event.logs

        except grpc.aio.AioRpcError as e:
            print(e)
            if e.code() == grpc.StatusCode.CANCELLED:
                return

            elif e.code() == grpc.StatusCode.UNAVAILABLE:
                return

            else:
                raise e