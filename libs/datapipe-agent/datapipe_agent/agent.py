import asyncio
import signal
import grpc
import os
import logging
import time

from dataclasses import dataclass
from datapipe.compute import DatapipeApp

from datapipe_router.types import DataFilter
from datapipe_router.pb2.storage_client_pb2 import StatusEvent, LogEvent
from datapipe_router.pb2.storage_agent_pb2 import SendRunStatusRequest, SendLogsRequest
from datapipe_router.pb2.storage_agent_pb2_grpc import StorageDatapipeServiceStub
from datapipe_router.pb2.router_agent_pb2_grpc import RouterDatapipeServiceStub
from datapipe_router.pb2.router_agent_pb2 import (
    DataEvent, 
    GraphEvent, 
    RunEvent,
    SendDataRequest, 
    SendGraphRequest,
    ServerEventsRequest,
    ServerEventsResponse,
    SendRunCreationStatusRequest,
    PingRequest,
)

from datapipe_agent.config import AgentSettings, RunnerType
from datapipe_agent.runners.base import StatusEvent as AgentStatusEvent, LogEvent as AgentLogEvent
from datapipe_agent.runners.local import LocalRunner
from datapipe_agent.runners.kubernates import KubernatesRunner
from datapipe_agent.libs.graph import get_pipeline_graph
from datapipe_agent.libs.table_data import get_table_data, TableDataSettings


CHANNEL_OPTIONS = [
    ('grpc.keepalive_time_ms', 30000),             # Send pings every 30 seconds if idle
    ('grpc.keepalive_timeout_ms', 10000),          # Wait 10 seconds for ping response
    ('grpc.keepalive_permit_without_calls', 1),    # Allow pings even if there are no active streams
    ('grpc.http2.max_pings_without_data', 0),      # Unlimited pings without sending data
]

CHECK_RECONNECT_STATE_DELAY = 1
RECONNECT_DELAY_SECONDS = 5
SEND_PING_DELAY = 10

logger = logging.getLogger("datapipe_agent")


@dataclass
class GRPCChannel:
    channel: grpc.Channel
    stub: object


class DatapipeAgent(DatapipeApp):
    def __init__(self, app: DatapipeApp, settings: AgentSettings):
        self.app = app
        self.settings = settings

        self._server_event_stream = None
        self._shitdown = False
        self._router_channel = None
        self._storage_channel = None
        self._runns = {}
        self._status_queue = asyncio.Queue()
        self._logs_queue = asyncio.Queue()

    def _get_router_grpc_stub(self):
        if self._router_channel is None:
            url = f'{self.settings.router.host}:{self.settings.router.port}'
            channel = grpc.aio.insecure_channel(url, options=CHANNEL_OPTIONS)
            self._router_channel = GRPCChannel(
                channel=channel,
                stub=RouterDatapipeServiceStub(channel)
            )

        return self._router_channel.stub

    def _get_storage_grpc_stub(self):
        if self._storage_channel is None:
            url = f'{self.settings.storage.host}:{self.settings.storage.port}'
            channel = grpc.aio.insecure_channel(url, options=CHANNEL_OPTIONS)
            self._storage_channel = GRPCChannel(
                channel=channel,
                stub=StorageDatapipeServiceStub(channel)
            )

        return self._storage_channel.stub

    async def send_data(self, request_id: str, data: DataEvent):
        request = data.request

        if request.filters:
            try:
                filters = DataFilter.from_bytes(request.filters).data
            except:
                logger.error(f"({request_id}) Incorrect filters data...")
                return
        else: 
            filters = {}

        if request.focus:
            try:
                focus = [
                    DataFilter.from_bytes(item).data
                    for item in request.focus
                ] 
            except:
                logger.error(f"({request_id}) Incorrect focus data...")
                return
        else: 
            focus = []

        settings = TableDataSettings(
            page=data.request.page,
            page_size=data.request.page_size,
            include_total=data.request.include_total,
            order=data.request.order,
            order_by=data.request.order_by,
            filters=filters,
            focus=focus
        )

        table_data = get_table_data(self.app.ds, self.app.catalog, request.table, settings)

        stub = self._get_router_grpc_stub()
        request = SendDataRequest(
            route_id=data.route_id,
            data=table_data.to_message()
        )

        await stub.SendData(request)

    async def send_graph(self, request_id: str, data: GraphEvent):
        labels = {data.label_key: data.value} if data.value else {}
        start = time.time()
        graph = get_pipeline_graph(self.app, labels)
        logger.info(f"({request_id}) Graph generated: {time.time() - start} s")
        
        stub = self._get_router_grpc_stub()
        request = SendGraphRequest(
            route_id=data.route_id,
            data=graph.to_message()
        )

        await stub.SendGraph(request)
        logger.info(f"({request_id}) Graph processed: {time.time() - start} s")

    async def run_status_handler(self):
        while True:
            event: AgentStatusEvent = await self._status_queue.get()

            if event:
                logger.info(f"({event.run_id}) Pipeline run status: {event.status}")

                stub = self._get_storage_grpc_stub()
                request = SendRunStatusRequest(
                    run_id=event.run_id,
                    status=StatusEvent(
                        timestamp=event.timestamp,
                        status=event.status
                    ),
                )
        
                await stub.SendRunStatus(request)

    async def run_logs_handler(self):
        while True:
            event: AgentLogEvent = await self._logs_queue.get()

            if event:
                stub = self._get_storage_grpc_stub()
                request = SendLogsRequest(
                    run_id=event.run_id,
                    log=LogEvent(
                        timestamp=event.timestamp,
                        sequence=event.sequence,
                        log=event.log
                    ),
                )
        
                await stub.SendLogs(request)

    async def run_pipeline(self, request_id: str, data: RunEvent):
        logger.info(f"({data.run_id}) Pipiline running...")

        try:
            labels = (
                [label.item for label in data.labels]
                if data.labels else []
            )

            if self.settings.runner_type == RunnerType.LOCAL:
                runner = LocalRunner(
                    data.run_id, 
                    self._status_queue, 
                    self._logs_queue, 
                    labels=labels,
                )
            elif self.settings.runner_type == RunnerType.KUBERNATES:
                runner = KubernatesRunner(
                    data.run_id, 
                    self._status_queue, 
                    self._logs_queue, 
                    self.settings.kubernates, 
                    labels=labels,
                    envs=os.environ,
                )
            else:
                raise ValueError(f"Unknow runner type: {self.settings.runner_type}")

        except Exception as e:
            stub = self._get_router_grpc_stub()
            request = SendRunCreationStatusRequest(
                route_id=data.route_id,
                state="error",
                error=str(e)
            )
    
            await stub.SendRunCreationStatus(request)

            logger.error(f"({data.run_id}) Run pipeline error: {str(e)}")


        self._runns[data.run_id] = runner
        stub = self._get_router_grpc_stub()
        request = SendRunCreationStatusRequest(
            route_id=data.route_id,
            state="ok",
            error=None
        )

        await stub.SendRunCreationStatus(request)
        await runner.run()

        del self._runns[data.run_id]

    async def process_command(self, command: ServerEventsResponse):
        active_field = command.WhichOneof("event")

        if active_field == "ping_event":
            return

        logger.info(f"Received server command [{command.request_id}]")

        if active_field == "data_event":
            asyncio.create_task(self.send_data(command.request_id, command.data_event))
        elif active_field == "graph_event":
            asyncio.create_task(self.send_graph(command.request_id, command.graph_event))
        elif active_field == "run_event":
            asyncio.create_task(self.run_pipeline(command.request_id, command.run_event))
        else:
            logger.warning("Command not found.")

    async def send_pings(self):
        while True:
            await asyncio.sleep(SEND_PING_DELAY)

            stub = self._get_router_grpc_stub()
            request = PingRequest(
                name=self.settings.name
            )

            response = await stub.SendPing(request)

            if self._server_event_stream and response.status != "ok":
                self._server_event_stream.cancel()
                self._server_event_stream = None

    async def run_agent(self):
        logger.info(f"Datapipe agent starting...")
        self._loop = asyncio.get_running_loop()
            
        # Register OS signals for clean termination (SIGINT = Ctrl+C, SIGTERM = systemctl stop)
        for sig in (signal.SIGINT, signal.SIGTERM):
            self._loop.add_signal_handler(
                sig, 
                lambda s=sig: asyncio.create_task(self.shutdown(s))
            )

        asyncio.create_task(self.run_status_handler())
        asyncio.create_task(self.run_logs_handler())
        asyncio.create_task(self.send_pings())

        logger.info(f"Datapipe agent started.")

        while True:
            try:
                logger.info(f"Initialize connection to server...")
                stub = self._get_router_grpc_stub()
                request = ServerEventsRequest(name=self.settings.name)
                
                self._server_event_stream = stub.GetStreamServerEvents(request)

                logger.info("Waiting server commands...")

                async for command in self._server_event_stream:
                    await self.process_command(command)
                
            except grpc.aio.AioRpcError as e:
                if e.code() == grpc.StatusCode.CANCELLED:
                    logger.error(f"RPC was cancelled: {e.details()}. Try to reconnect...")
                    await asyncio.sleep(RECONNECT_DELAY_SECONDS)

                elif e.code() == grpc.StatusCode.UNAVAILABLE:
                    logger.error("Server unavailable. Try to reconnect...")
                    await asyncio.sleep(RECONNECT_DELAY_SECONDS)

                else:
                    raise e

            except asyncio.CancelledError:
                if self._shitdown:
                    logger.error(f"Datapipe agent received cancellation request.")
                    break 

                logger.error("Server disconect client. Try to reconnect...")
                continue
            
    async def shutdown(self, signal_name):
        """Safely cancels all running background tasks and stops the loop."""
        logger.info(f"Received exit signal {signal_name.name}...")
        
        # Grab all tasks running in the event loop except the shutdown task itself
        self._shitdown = True
        tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        
        if not tasks:
            self._loop.stop()
            return

        logger.info(f"Cancelling {len(tasks)} outstanding background tasks...")
        for task in tasks:
            task.cancel()
            
        # Wait for all tasks to acknowledge cancellation gracefully
        await asyncio.gather(*tasks, return_exceptions=False)

        logger.info("All background tasks clean. Stopping event loop.")
        self._loop.stop()


    

     